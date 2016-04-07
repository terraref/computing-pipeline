#!/usr/bin/python

""" GANTRY MONITOR SERVICE
    This will load parameters from the configFile defined below,
    and begin monitoring the specified gantry file directory for
    new files. When transfer from the gantry to the directory
    is complete, this service will initiate the Globus transfer
    and send notification to the API at the receiving end.

    This service has 2 objects in memory:
      pendingTransfers (files queued for Globus transfer)
      activeTasks (Globus transfer tasks of 1+ files)

    When criteria for transfer are met, a batch of files is
    sent in a Globus task and the pendingTransfers queue is flushed.
    This will continually check with the NCSA Globus monitor API
    until that task is completed; then the gantry files are
    queued for deletion.
"""

import os, shutil, json, time, datetime, thread, copy, subprocess, atexit, collections, fcntl
import requests
from io import BlockingIOError
from flask import Flask, request, Response
from flask.ext import restful
from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, ClientError, goauth

rootPath = "/home/gantrymonitor/"

config = {}
logFile = None

"""pendingTransfers tracks files prepped for transfer, by dataset:
{
    "dataset": {
        "files": {
            "filename1": {
                "name": "filename1",
                "md": {},
                "md_name": "name_of_metadata_file"
                "md_path": "folder_containing_metadata_file"},
            "filename2": {...},
            ...
        },
        "md": {},
        "md_path": "folder_containing_metadata.json"
    },
    "dataset2": {...},
...}"""
pendingTransfers = {}

"""activeTasks tracks Globus transfers is of the format:
{"globus_id": {
    "globus_id":                globus job ID of upload
    "contents": {...},          a pendingTransfers object that was sent (see above)
    "started":                  timestamp when task was sent to Globus
    "completed":                timestamp when task was completed (including errors and cancelled tasks)
    "status":                   can be "IN PROGRESS", "DONE", "ABORTED", "ERROR"
}, {...}, {...}, ...}"""
activeTasks = {}

app = Flask(__name__)
api = restful.Api(app)

# ----------------------------------------------------------
# SHARED UTILS
# ----------------------------------------------------------
def openLog():
    global logFile

    logPath = config["log_path"]
    logFile = open(logPath, 'w')

def closeLog():
    logFile.close()

"""Attempt to lock a file so API and monitor don't write at once, and wait if unable"""
def lockFile(f):
    # From http://tilde.town/~cristo/file-locking-in-python.html
    while True:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError as e:
            # Try again in 1/10th of a second
            time.sleep(0.1)

"""Nested update of python dictionaries for config parsing"""
def updateNestedDict(existing, new):
    # Adapted from http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    for k, v in new.iteritems():
        if isinstance(existing, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = updateNestedDict(existing.get(k, {}), v)
                existing[k] = r
            else:
                existing[k] = new[k]
        else:
            existing = {k: new[k]}
    return existing

"""Print log message to console and write it to log file"""
def log(message, type="INFO"):
    print("["+type+"] "+message)
    logFile.write("["+type+"] "+message+"\n")

"""Return small JSON object with information about monitor health"""
def getStatus():
    pendingFileCount = 0
    for ds in pendingTransfers:
        if 'files' in pendingTransfers[ds]:
            for f in pendingTransfers[ds]['files']:
                pendingFileCount += 1

    createdTasks = len(activeTasks)

    completedTasks = 0
    for root, dirs, filelist in os.walk(config['completed_tasks_path']):
        completedTasks += len(filelist)

    return {
        "pending_file_transfers": pendingFileCount,
        "globus_tasks_sent": createdTasks,
        "completed_globus_tasks": completedTasks
    }

"""Load contents of .json file into a JSON object"""
def loadJsonFile(filename):
    f = open(filename)
    jsonObj = json.load(f)
    f.close()
    return jsonObj

"""Load active or pending tasks from file into memory"""
def loadTasksFromDisk(filePath):
    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(filePath):
        if os.path.exists(filePath+".backup"):
            log("...loading tasks from "+filePath+".backup")
            shutil.copyfile(filePath+".backup", filePath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(filePath, 'w')
            f.write("{}")
            f.close()
    else:
        log("...loading tasks from "+filePath)

    return loadJsonFile(filePath)

"""Write active or pending tasks from memory into file"""
def writeTasksToDisk(filePath, taskObj):
    # Write current file to backup location before writing current file
    log("...writing tasks to "+filePath)

    if os.path.exists(filePath):
        shutil.move(filePath, filePath+".backup")

    f = open(filePath, 'w')
    lockFile(f)
    f.write(json.dumps(taskObj))
    f.close()

"""Write a completed task onto disk in appropriate folder hierarchy"""
def writeCompletedTransferToDisk(transfer):
    completedPath = config['completed_tasks_path']
    taskID = transfer['globus_id']

    # e.g. TaskID "eaca1f1a-d400-11e5-975b-22000b9da45e"
    #   = <completedPath>/ea/ca/1f/1a/eaca1f1a-d400-11e5-975b-22000b9da45e.json

    # Create root directory if necessary
    if not os.path.exists(completedPath):
        os.mkdir(completedPath)
    # Create nested hierarchy folders if needed, to hopefully avoid a long flat list
    treeLv1 = os.path.join(completedPath, taskID[:2])
    treeLv2 = os.path.join(treeLv1, taskID[2:4])
    treeLv3 = os.path.join(treeLv2, taskID[4:6])
    treeLv4 = os.path.join(treeLv3, taskID[6:8])
    for dir in [treeLv1, treeLv2, treeLv3, treeLv4]:
        if not os.path.exists(dir):
            os.mkdir(dir)

    # Write to json file with task ID as filename
    dest = os.path.join(treeLv4, taskID+".json")
    log("...writing completed transfer to "+dest)
    f = open(dest, 'w')
    f.write(json.dumps(transfer))
    f.close()

"""Move file from src to dest directory, creating dirs as needed"""
def moveLocalFile(srcPath, destPath, filename):
    log("...moving "+filename+" to "+destPath)

    if not os.path.isdir(destPath):
        os.makedirs(destPath)
    shutil.move(os.path.join(srcPath, filename),
                os.path.join(destPath, filename))

"""Clear out any datasets from pendingTransfers without files or metadata"""
def cleanPendingTransfers():
    toRemove = []
    for ds in pendingTransfers:
        keep = False
        if 'files' in pendingTransfers[ds]:
            keep = pendingTransfers[ds]['files'] != {}
        if 'md' in pendingTransfers[ds]:
            keep = pendingTransfers[ds]['md'] != {}
        if not keep:
            toRemove.append(ds)

    for ds in toRemove:
        del pendingTransfers[ds]

"""Return true if a file is currently part of an active transfer"""
def filenameInActiveTasks(filename):
    for globusID in activeTasks:
        for ds in activeTasks[globusID]['contents']:
            dsobj = activeTasks[globusID]['contents'][ds]
            if 'files' in dsobj:
                for f in dsobj['files']:
                    if f == filename:
                        return True

    return False

"""Add a particular file to pendingTransfers, checking for metadata first"""
def addFileToPendingTransfers(f):
    global pendingTransfers

    gantryDirPath = f.replace(config['gantry']['incoming_files_path'], "")
    pathParts = gantryDirPath.split("/")

    filename = pathParts[-1]
    sensorname = pathParts[-4] if len(pathParts)>3 else "unknown_sensor"
    timestamp = pathParts[-2]  if len(pathParts)>1 else "unknown_time"
    datasetID = sensorname +" "+timestamp
    gantryDirPath = gantryDirPath.replace(filename, "")

    if filename.find("metadata.json") == -1:
        pendingTransfers = updateNestedDict(pendingTransfers, {
            datasetID: {
                "files": {
                    filename: {
                        "name": filename,
                        "path": gantryDirPath[1:] if gantryDirPath[0]=="/" else gantryDirPath
                    }
                }
            }
        })
        log("file queued for transfer: "+f)

    else:
        # Found metadata.json, assume it is for dataset
        pendingTransfers = updateNestedDict(pendingTransfers, {
            datasetID: {
                "md": loadJsonFile(f),
                "md_path": gantryDirPath[1:] if gantryDirPath[0 ]== "/" else gantryDirPath
            }
        })
        log("dataset metadata found for: "+datasetID)

# ----------------------------------------------------------
# API COMPONENTS
# ----------------------------------------------------------
""" /files
Add a file to the transfer queue manually so it can be sent to NCSA Globus"""
class TransferQueue(restful.Resource):

    def post(self):
        req = request.get_json(force=True)

        # Single file path entry under 'path'
        if 'path' in req:
            addFileToPendingTransfers(req['path'])

        # Multiple file path entries under 'paths'
        if 'paths' in req:
            for f in req['paths']:
                addFileToPendingTransfers(f)

        writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)
        return 201

""" / status
Return basic information about monitor for health checking"""
class MonitorStatus(restful.Resource):

    def get(self):
        return getStatus(), 200

api.add_resource(TransferQueue, '/files')
api.add_resource(MonitorStatus, '/status')

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access token for config account"""
def generateAuthToken():
    log("generating auth token for "+config['globus']['username'])
    t = goauth.get_access_token(
            username=config['globus']['username'],
            password=config['globus']['password']
    ).token
    config['globus']['auth_token'] = t
    log("...generated: "+t)

"""Generate a submission ID that can be used to avoid double-submitting"""
def generateGlobusSubmissionID():
    try:
        api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
        status_code, status_message, submission_id = api.submission_id()
    except (APIError, ClientError) as e:
        # Try refreshing auth token and retrying
        generateAuthToken()
        api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
        status_code, status_message, submission_id = api.submission_id()

    if status_code == 200:
        return submission_id['value']
    else:
        return "UNKNOWN ("+status_code+": "+status_message+")"

"""Check for files ready for transmission and return list"""
def getGantryFilesForTransfer(gantryDir):
    transferQueue = {}

    # Get list of files last modified more than X minutes ago
    #fileAge = config['gantry']['min_file_age_for_transfer_mins']
    #foundFiles = subprocess.check_output(["find", gantryDir, "-mmin", "+"+fileAge, "-type", "f", "-print"]).split("\n")

    # Get list of files from FTP log
    foundFiles = getTransferQueueFromLogs()

    for f in foundFiles:
        # Get dataset info & path details from found file
        gantryDirPath = f.replace(gantryDir,"")
        pathParts = gantryDirPath.split("/")
        filename = pathParts[-1]
        sensorname = pathParts[-4] if len(pathParts)>3 else "unknown_sensor"
        timestamp = pathParts[-2]  if len(pathParts)>1 else "unknown_time"
        datasetID = sensorname +" "+timestamp
        gantryDirPath = gantryDirPath.replace(filename, "")

        # Skip hidden/system files
        if f == "" or filename[0] == ".":
            continue

        # Check add entry for this dataset if necessary
        if datasetID not in pendingTransfers:
            pendingTransfers[datasetID] = {}
            pendingTransfers[datasetID]['files'] = {}
        if datasetID not in transferQueue:
            transferQueue[datasetID] = {}
            transferQueue[datasetID]['files'] = {}

        if filename not in pendingTransfers[datasetID]['files'] and not filenameInActiveTasks(filename):
            if filename.find("metadata.json") == -1:
                # TODO: Check for .json file with same name as current file - assume metadata if so
                transferQueue[datasetID]['files'][filename] = {
                    "name": filename,
                    "path": gantryDirPath[1:] if gantryDirPath[0 ]== "/" else gantryDirPath
                    # "md": {}, only include md key if present
                    # "md_name": "" metadata filename
                    # "md_path": "" metadata folder
                }
            else:
                # Found metadata.json, assume it is for dataset
                transferQueue[datasetID]['md'] = loadJsonFile(f)
                transferQueue[datasetID]['md_path'] = gantryDirPath[1:] if gantryDirPath[0 ]== "/" else gantryDirPath

    return transferQueue

"""Check FTP log files to determine new files that were successfully moved to staging area"""
def getTransferQueueFromLogs():
    transferQueue = {}

    logDir = config["gantry"]["ftp_log_path"]
    previousLine = "" # TODO: Stash in a log file
    lastLineRead = ""
    currLog = os.path.join(logDir, "xferlog")
    backLog = 0

    #Tue Apr  5 12:35:58 2016 1 ::ffff:150.135.84.81 4061858 /gantry_data/LemnaTec/EnvironmentLogger/2016-04-05/2016-04-05_12-34-58_enviromentlogger.json b _ i r lemnatec ftp 0 * c
    # compare timestamps & filenames
    
    # for line in iter(fileobj.readline, ''):
    # pos = file.tell()
    # file.seek(pos)
    #

    if lastLine != "":
        foundLines = subprocess.check_output(["grep","-a",lastLine]).split("\n")

    foundResumePoint = False
    handledBackLog = True
    while not (foundResumePoint and handledBackLog):
        with open(currLog, 'r+') as f:
            for line in f:
                if line == lastLine:
                    foundResumePoint = True

                elif foundResumePoint:
                    # We're past the last queue's line, so capture these
                    lastLineRead = line
                    vals = line.split(" ")
                    if vals[-1].replace("\n","") == 'c':    # c = complete, i = incomplete
                        path = vals[-10]
                        pathParts = path.split("/")
                        filename = pathParts[-1]
                        # TODO: These indices may change depending on sensor?
                        sensorname = pathParts[-3] if len(pathParts)>3 else "unknown_sensor"
                        timestamp = pathParts[-2]  if len(pathParts)>1 else "unknown_time"
                        datasetID = sensorname+" "+timestamp
                        transferQueue[datasetID]['files'][filename] = {
                            "name": filename,
                            "path": path
                        }

        # If we didn't find last line in this file, look into the previous file
        if not foundResumePoint:
            handledBackLog = False
            backLog += 1
            currLog = os.path.join(logDir, "xferlog-"+str(backLog))
            if not os.path.exists(currLog):
                return {} # TODO: Didn't find last line. Read entire file(s)? Check timestamps? etc.

        # If we found last line in a previous file, climb back up to current file and get its contents too
        elif backLog > 0:
            backLog -= 1
            currLogName = "xferlog-"+str(backLog) if backLog > 0 else "xferlog"
            currLog = os.path.join(logDir, currLogName)

        # If we found the line and handled all backlogged files, we're ready to go
        else:
            handledBackLog = True

    # TODO: Write lastLineRead to a log file

    return transferQueue


    # TODO: Use FTP logfile in /var/log/xferlog to find 'c' completed files and queue them
    # grep -a on latest line stored to find the last thing I read - store timestamp, if not in current, check previous (.gz) pipe thru zcat
    # look in /var/log/xferlog for last line
    # if not found, look at file n-1 until found
    # some files will be in a g-zip archive

"""Initiate Globus transfer with batch of files and add to activeTasks"""
def initializeGlobusTransfers():
    global pendingTransfers

    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
    submissionID = generateGlobusSubmissionID()

    # Prepare transfer object
    transferObj = Transfer(submissionID,
                           config['globus']['source_endpoint_id'],
                           config['globus']['destination_endpoint_id'])

    queueLength = 0
    for ds in pendingTransfers:
        if "files" in pendingTransfers[ds]:
            # Add files from each dataset
            for f in pendingTransfers[ds]['files']:
                fobj = pendingTransfers[ds]['files'][f]
                src_path = os.path.join(config['gantry']['incoming_files_path'], fobj["path"], fobj["name"])
                dest_path = os.path.join(config['globus']['destination_path'], fobj["path"],  fobj["name"])
                transferObj.add_item(src_path, dest_path)
                queueLength += 1
        elif "md" in pendingTransfers[ds]:
            # We have metadata for a dataset, but no files. Just send metadata separately.
            sendMetadataToMonitor(ds, pendingTransfers[ds]['md'])

    if queueLength > 0:
        # Send transfer to Globus
        try:
            status_code, status_message, transfer_data = api.transfer(transferObj)
        except (APIError, ClientError) as e:
            # Try refreshing auth token and retrying
            generateAuthToken()
            api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
            status_code, status_message, transfer_data = api.transfer(transferObj)

        # Notify NCSA monitor of new task, and add to activeTasks for logging
        if status_code == 200 or status_code == 202:
            globusID = transfer_data['task_id']

            log("globus transfer task started: "+globusID+" ("+str(queueLength)+" files)")

            activeTasks[globusID] = {
                "globus_id": globusID,
                "contents": pendingTransfers,
                "started": str(datetime.datetime.now()),
                "status": "IN PROGRESS"
            }
            writeTasksToDisk(config['active_tasks_path'], activeTasks)

            notifyMonitorOfNewTransfer(globusID, pendingTransfers)

            pendingTransfers = {}
            writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)
        else:
            log("globus initialization failed for "+ds+" ("+status_code+": "+status_message+")", "ERROR")

"""Send message to NCSA Globus monitor API that a new task has begun"""
def notifyMonitorOfNewTransfer(globusID, contents):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    sess.post(config['ncsa_api']['host']+"/tasks", data=json.dumps({
        "user": config['globus']['username'],
        "globus_id": globusID,
        "contents": contents
    }))

"""Send message to NCSA Globus monitor API with metadata for a dataset, without other files"""
def sendMetadataToMonitor(datasetName, metadata):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    # Check with Globus monitor rather than Globus itself, to make sure file was handled properly before deleting from src
    log("sending metadata for "+datasetName)
    status = sess.post(config['ncsa_api']['host']+"/metadata", data=json.dumps({
        "user": config['globus']['username'],
        "dataset": datasetName,
        "md": metadata
    }))

    return status

"""Contact NCSA Globus monitor API to check whether task was completed successfully"""
def getTransferStatusFromMonitor(globusID):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    # Check with Globus monitor rather than Globus itself, to make sure file was handled properly before deleting from src
    st = sess.get(config['ncsa_api']['host']+"/tasks/"+globusID)

    if st.status_code == 200:
        return json.loads(st.text)['status']
    else:
        log("monitor status check failed for task "+globusID+" ("+st.status_code+": "+st.status_message+")", "ERROR")
        return "UNKNOWN"

"""Continually monitor gantry directory for new files to transmit"""
def gantryMonitorLoop():
    # Prepare timers for tracking how often different refreshes are executed
    gantryWait = config['gantry']['file_check_frequency_secs'] # look for new files to send
    apiWait = config['ncsa_api']['api_check_frequency'] # check status of sent files
    authWait = config['globus']['authentication_refresh_frequency_secs'] # renew globus auth

    while True:
        time.sleep(1)
        gantryWait -= 1
        apiWait -= 1
        authWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if gantryWait <= 0:
            pendingTransfers.update(getGantryFilesForTransfer(config['gantry']['incoming_files_path']))

            # Clean up the pending object of straggling keys, then initialize Globus transfer
            cleanPendingTransfers()
            if pendingTransfers != {}:
                writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)
                initializeGlobusTransfers()
            # Reset wait to check gantry incoming directory again
            gantryWait = config['gantry']['file_check_frequency_secs']
            writeTasksToDisk(config["status_log_path"], getStatus())

        # Check with NCSA Globus monitor API for completed transfers
        if apiWait <= 0:
            # Use copy of task list so it doesn't change during iteration
            currentActiveTasks = copy.copy(activeTasks)
            for globusID in currentActiveTasks:
                task = activeTasks[globusID]

                globusStatus = getTransferStatusFromMonitor(globusID)
                if globusStatus in ["SUCCEEDED", "FAILED"]:
                    log("status update received for "+globusID+": "+globusStatus)
                    task['status'] = globusStatus
                    task['completed'] = str(datetime.datetime.now())

                    # Write out results log
                    writeCompletedTransferToDisk(task)

                    # Move files (and metadata files if needed) to staging area for deletion
                    if globusStatus == "SUCCEEDED":
                        deleteDir = config['gantry']['deletion_queue']
                        if deleteDir != "":
                            for ds in task['contents']:
                                if 'files' in task['contents'][ds]:
                                    for f in task['contents'][ds]['files']:
                                        fobj = task['contents'][ds]['files'][f]
                                        moveLocalFile(os.path.join(config['gantry']['incoming_files_path'], fobj['path']),
                                                      os.path.join(deleteDir, fobj['path']), fobj['name'])
                                        if 'md'in fobj:
                                            moveLocalFile(os.path.join(config['gantry']['incoming_files_path'], fobj['md_path']),
                                                          os.path.join(deleteDir, fobj['md_path']), fobj['md_name'])
                                if 'md' in task['contents'][ds]:
                                    dsobj = task['contents'][ds]
                                    moveLocalFile(os.path.join(config['gantry']['incoming_files_path'], dsobj['md_path']),
                                                  os.path.join(deleteDir, dsobj['md_path']), "metadata.json")

                            # Crawl and remove empty directories
                            log("...removing empty directories in "+config['gantry']['incoming_files_path'])
                            subprocess.call(["find", config['gantry']['incoming_files_path'], "-type", "d", "-empty", "-delete"])

                    del activeTasks[globusID]
                    writeTasksToDisk(config['active_tasks_path'], activeTasks)

            # Reset timer to check NCSA api for transfer updates again
            apiWait = config['ncsa_api']['api_check_frequency']
            writeTasksToDisk(config["status_log_path"], getStatus())

        # Refresh Globus auth tokens
        if authWait <= 0:
            generateAuthToken()
            authWait = config['globus']['authentication_refresh_frequency_secs']

if __name__ == '__main__':
    # Try to load custom config file, falling back to default values where not overridden
    config = loadJsonFile(os.path.join(rootPath, "config_default.json"))
    if os.path.exists(os.path.join(rootPath, "data/config_custom.json")):
        print("...loading configuration from config_custom.json")
        config = updateNestedDict(config, loadJsonFile(os.path.join(rootPath, "data/config_custom.json")))
    else:
        print("...no custom configuration file found. using default values")
    openLog()
    atexit.register(closeLog)
    generateAuthToken()

    # TODO: Crash checks - are we writing enough logs at the right times to recover no matter when?
    # TODO: How to handle big errors, e.g. NCSA API not responding? admin email notification?

    # Load any previous active/pending transfers
    activeTasks = loadTasksFromDisk(config['active_tasks_path'])
    pendingTransfers = loadTasksFromDisk(config['pending_transfers_path'])

    # Create thread for service to begin monitoring
    log("*** Service now monitoring gantry directory ***")
    thread.start_new_thread(gantryMonitorLoop, ())

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    log("*** API now listening on "+config['api']['ip_address']+":"+apiPort+" ***")
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

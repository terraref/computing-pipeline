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

import os, shutil, json, time, datetime, thread, copy, subprocess, atexit, collections, fcntl, re, gzip
import requests
from io import BlockingIOError
from flask import Flask, request, Response
from flask.ext import restful
from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, ClientError, goauth

rootPath = "/home/gantrymonitor/"

config = {}
logFile = None

# These are used by the FTP log reader to track progress, and included in status report
status_lastFTPLogLine = ""

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

    # If there's a current log file, store it as log1.txt, log2.txt, etc.
    if os.path.exists(logPath):
        i = 1
        backupLog = logPath.replace(".txt", "_"+str(i)+".txt")
        while os.path.exists(backupLog):
            i+=1
            backupLog = logPath.replace(".txt", "_"+str(i)+".txt")
        shutil.move(logPath, backupLog)

    logFile = open(logPath, 'w+')

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
        "active_globus_tasks": createdTasks,
        "completed_globus_tasks": completedTasks,
        "last_ftp_log_line_read": status_lastFTPLogLine
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
    # Iterate across a copy since we'll be changing object
    allPendingTransfers = copy.deepcopy(pendingTransfers)
    for ds in allPendingTransfers:
        dsobj = allPendingTransfers[ds]
        if 'files' in dsobj and len(dsobj['files']) == 0:
            del pendingTransfers[ds]['files']
        if 'md' in dsobj and len(dsobj['md']) == 0:
            del pendingTransfers[ds]['md']
        if 'files' not in pendingTransfers[ds] and 'md' not in pendingTransfers[ds]:
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

"""Take line of FTP transfer log and parse a datetime object from it"""
def parseDateFromFTPLogLine(line):
    # Example log line:
    #Tue Apr  5 12:35:58 2016 1 ::ffff:150.135.84.81 4061858 /gantry_data/LemnaTec/EnvironmentLogger/2016-04-05/2016-04-05_12-34-58_enviromentlogger.json b _ i r lemnatec ftp 0 * c

    months = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }

    l = line.split(" ")
    if len(l) > 6:
        YY = int(l[5])      # year, e.g. 2016
        MM = months[l[1]]   # month, e.g. 4
        DD = int(l[3])      # day of month, e.g. 5
        hh = int(l[4].split(':')[0])  # hours, e.g. 12
        mm = int(l[4].split(':')[1])  # minutes, e.g. 35
        ss = int(l[4].split(':')[2])  # seconds, e.g. 58

        return datetime.datetime(YY, MM, DD, hh, mm, ss)
    else:
        # TODO: How to handle unparseable log line?
        return datetime.datetime(1900, 1, 1, 1, 1, 1)

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
        try:
            # Try refreshing auth token and retrying
            generateAuthToken()
            api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
            status_code, status_message, submission_id = api.submission_id()
        except (APIError, ClientError) as e:
            log("problem generating submission ID for globus transfer", "ERROR")
            status_code = 503

    if status_code == 200:
        return submission_id['value']
    else:
        return None

"""Check for files ready for transmission and return list"""
def getGantryFilesForTransfer(gantryDir):
    transferQueue = {}

    # Get list of files last modified more than X minutes ago
    #fileAge = config['gantry']['min_file_age_for_transfer_mins']
    #foundFiles = subprocess.check_output(["find", gantryDir, "-mmin", "+"+fileAge, "-type", "f", "-print"]).split("\n")

    # Get list of files from FTP log
    foundFiles = getNewFilesFromFTPLogs()

    for f in foundFiles:
        # Get dataset info & path details from found file
        gantryDirPath = f.replace(gantryDir,"")
        pathParts = gantryDirPath.split("/")
        filename = pathParts[-1]
        sensorname = pathParts[-4] if len(pathParts)>3 else "unknown_sensor"
        timestamp = pathParts[-2]  if len(pathParts)>1 else "unknown_time"
        datasetID = sensorname +" - "+timestamp
        gantryDirPath = gantryDirPath.replace(filename, "")

        # Skip hidden/system files
        if f == "" or filename[0] == ".":
            continue

        # Check add entry for this dataset if necessary
        if datasetID not in pendingTransfers: pendingTransfers[datasetID] = {}
        if 'files' not in pendingTransfers[datasetID]: pendingTransfers[datasetID]['files'] = {}
        if datasetID not in transferQueue: transferQueue[datasetID] = {}
        if 'files' not in transferQueue[datasetID]: transferQueue[datasetID]['files'] = {}

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
def getNewFilesFromFTPLogs():
    global status_lastFTPLogLine
    foundFiles = []

    logDir = config["gantry"]["ftp_log_path"]
    if logDir == "":
        # Don't perform a scan if no log file is defined
        return foundFiles

    # Example log line:
    #Tue Apr  5 12:35:58 2016 1 ::ffff:150.135.84.81 4061858 /gantry_data/LemnaTec/EnvironmentLogger/2016-04-05/2016-04-05_12-34-58_enviromentlogger.json b _ i r lemnatec ftp 0 * c
    lastLine = copy.copy(status_lastFTPLogLine)
    lastReadTime = parseDateFromFTPLogLine(lastLine)

    # TODO: Put this whole method in a separate thread so reading log file doesn't slow down initializing pending transfers
    currLog = os.path.join(logDir, "xferlog")
    backLog = 0

    foundResumePoint = False
    handledBackLog = True
    while not (foundResumePoint and handledBackLog):
        with (open(currLog, 'r+') if currLog.find(".gz")==-1 else gzip.open(currLog, 'r+')) as f:
            # If no most recent scanned line available, just start from beginning of current log file
            if lastLine == "":
                initialLine = False
                foundResumePoint = True
            else:
                initialLine = True

            for line in f:
                line = line.rstrip()
                if initialLine and handledBackLog:
                    firstLineTime = parseDateFromFTPLogLine(line)
                    if firstLineTime <= lastReadTime:
                        # File begins before most recent line - should be scanned
                        initialLine = False
                    elif firstLineTime > lastReadTime:
                        # File begins after most recent line - need to go back 1 file at least
                        break

                if line == lastLine:
                    foundResumePoint = True

                elif foundResumePoint:
                    # We're past the last scanned line, so capture these lines if complete & ending in 'c'
                    if re.search('ftp \d \* c', line.rstrip()):
                        status_lastFTPLogLine = line
                        # Extract filename from log entry, after an IP address and a number (byte count?)
                        fnameRegex = '::ffff:\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3} \d+ ((\/?.)+) +\w _'
                        fname = re.search(fnameRegex, line)
                        if fname:
                            foundFiles.append(fname.group(1).rstrip())

        # If we didn't find last line in this file, look into the previous file
        if not foundResumePoint:
            handledBackLog = False
            backLog += 1
            currLog = os.path.join(logDir, "xferlog-"+str(backLog))
            if not os.path.exists(currLog):
                # Check for gzipped verison as well
                currLog = os.path.join(logDir, "xferlog-"+str(backLog)+".gz")
                if not os.path.exists(currLog):
                    # Didn't find any previous logs, so just start with current one
                    currLog = os.path.join(logDir, "xferlog")
                    foundResumePoint = True

        # If we found last line in a previous file, climb back up to current file and get its contents too
        elif backLog > 0:
            backLog -= 1
            currLogName = "xferlog-"+str(backLog) if backLog > 0 else "xferlog"
            currLog = os.path.join(logDir, currLogName)
            if not os.path.exists(currLog):
                # Probably a gzipped version instead
                currLogName = "xferlog-"+str(backLog)+".gz" if backLog > 0 else "xferlog.gz"
                currLog = os.path.join(logDir, currLogName)

        # If we found the line and handled all backlogged files, we're ready to go
        else:
            handledBackLog = True

    return foundFiles

"""Initiate Globus transfer with batch of files and add to activeTasks"""
def initializeGlobusTransfers():
    global pendingTransfers

    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
    submissionID = generateGlobusSubmissionID()

    if submissionID:
        # Prepare transfer object
        transferObj = Transfer(submissionID,
                               config['globus']['source_endpoint_id'],
                               config['globus']['destination_endpoint_id'],
                               preserve_timestamp=True)

        queueLength = 0
        sentSomeMd = False
        remainingPendingTransfers = copy.deepcopy(pendingTransfers)
        currentTransferBatch = {}

        for ds in pendingTransfers:
            if "files" in pendingTransfers[ds]:
                if ds not in currentTransferBatch:
                    currentTransferBatch[ds] = {}
                    currentTransferBatch[ds]['files'] = {}
                # Add files from each dataset
                for f in pendingTransfers[ds]['files']:
                    if queueLength < config['globus']['max_transfer_file_count']:
                        fobj = pendingTransfers[ds]['files'][f]
                        src_path = os.path.join(config['gantry']['incoming_files_path'], fobj["path"], fobj["name"])
                        dest_path = os.path.join(config['globus']['destination_path'], fobj["path"],  fobj["name"])
                        transferObj.add_item(src_path, dest_path)

                        # remainingTransfers will have leftover data once max Globus transfer size is met
                        queueLength += 1
                        currentTransferBatch[ds]['files'][f] = fobj
                        del remainingPendingTransfers[ds]['files'][f]
                if "md" in pendingTransfers[ds]:
                    currentTransferBatch[ds]['md'] = pendingTransfers[ds]['md']
                    del remainingPendingTransfers[ds]['md']

            elif "md" in pendingTransfers[ds]:
                # We have metadata for a dataset, but no files. Just send metadata separately.
                mdxfer = sendMetadataToMonitor(ds, pendingTransfers[ds]['md'])
                # Leave metadata in pending if it wasn't successfully posted to Clowder
                if mdxfer.status_code == 200:
                    sentSomeMd = True
                    # Otherwise remove dataset entry since we already know there are no files
                    del remainingPendingTransfers[ds]

        if queueLength > 0:
            # Send transfer to Globus
            try:
                status_code, status_message, transfer_data = api.transfer(transferObj)
            except (APIError, ClientError) as e:
                try:
                    # Try refreshing auth token and retrying
                    generateAuthToken()
                    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
                    status_code, status_message, transfer_data = api.transfer(transferObj)
                except (APIError, ClientError) as e:
                    log("problem initializing Globus transfer", "ERROR")
                    status_code = 503
                    status_message = e

            if status_code == 200 or status_code == 202:
                # Notify NCSA monitor of new task, and add to activeTasks for logging
                globusID = transfer_data['task_id']

                log("Globus transfer task started: "+globusID+" ("+str(queueLength)+" files)")

                activeTasks[globusID] = {
                    "globus_id": globusID,
                    "contents": currentTransferBatch,
                    "started": str(datetime.datetime.now()),
                    "status": "IN PROGRESS"
                }
                writeTasksToDisk(config['active_tasks_path'], activeTasks)

                notifyMonitorOfNewTransfer(globusID, currentTransferBatch)

                pendingTransfers = remainingPendingTransfers
                writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)
            else:
                # If failed, leave pending list as-is and try again on next iteration (e.g. in 180 seconds)
                log("globus transfer failed for "+ds+" ("+str(status_code)+": "+str(status_message)+")", "ERROR")
                return
        elif sentSomeMd:
            # If metadata was sent there was still activity, so update pending transfers
            pendingTransfers = remainingPendingTransfers
            writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)

        cleanPendingTransfers()
        if pendingTransfers != {}:
            # If pendingTransfers not empty, we still have remaining files and need to start more Globus transfers
            initializeGlobusTransfers()

"""Send message to NCSA Globus monitor API that a new task has begun"""
def notifyMonitorOfNewTransfer(globusID, contents):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    log("notifying Globus monitor of "+globusID)
    try:
        status = sess.post(config['ncsa_api']['host']+"/tasks", data=json.dumps({
            "user": config['globus']['username'],
            "globus_id": globusID,
            "contents": contents
        }))
        return status
    except requests.ConnectionError as e:
        log("cannot connect to NCSA API", "ERROR")
        return {'status_code':503}

"""Send message to NCSA Globus monitor API with metadata for a dataset, without other files"""
def sendMetadataToMonitor(datasetName, metadata):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    # Check with Globus monitor rather than Globus itself, to make sure file was handled properly before deleting from src
    log("sending metadata for "+datasetName)
    try:
        status = sess.post(config['ncsa_api']['host']+"/metadata", data=json.dumps({
            "user": config['globus']['username'],
            "dataset": datasetName,
            "md": metadata
        }))
        return status
    except requests.ConnectionError as e:
        log("cannot connect to NCSA API", "ERROR")
        return {'status_code':503}

"""Contact NCSA Globus monitor API to check whether task was completed successfully"""
def getTransferStatusFromMonitor(globusID):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    # Check with Globus monitor rather than Globus itself, to make sure file was handled properly before deleting from src
    try:
        st = sess.get(config['ncsa_api']['host']+"/tasks/"+globusID)
        if st.status_code == 200:
            return json.loads(st.text)['status']
        elif st.status_code == 404:
            return "NOT FOUND"
        else:
            log("monitor status check failed for task "+globusID+" ("+str(st.status_code)+": "+st.status_message+")", "ERROR")
            return "UNKNOWN"
    except requests.ConnectionError as e:
        log("cannot connect to NCSA API", "ERROR")
        return "NOT FOUND"

"""Continually initiate transfers from pending queue and contact NCSA API for status updates"""
def globusMonitorLoop():
    # Prepare timers for tracking how often different refreshes are executed
    globusWait = config['gantry']['globus_transfer_frequency_secs'] # bundle pending files and transfer
    apiWait = config['ncsa_api']['api_check_frequency_secs'] # check status of sent files
    authWait = config['globus']['authentication_refresh_frequency_secs'] # renew globus auth

    while True:
        time.sleep(1)
        globusWait -= 1
        apiWait -= 1
        authWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if globusWait <= 0:
            # Clean up the pending object of straggling keys, then initialize Globus transfers
            cleanPendingTransfers()
            if pendingTransfers != {}:
                writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)
                initializeGlobusTransfers()

            # Reset wait to check gantry incoming directory again
            globusWait = config['gantry']['globus_transfer_frequency_secs']
            writeTasksToDisk(config["status_log_path"], getStatus())

        # Check with NCSA Globus monitor API for completed transfers
        if apiWait <= 0:
            # Use copy of task list so it doesn't change during iteration
            currentActiveTasks = copy.deepcopy(activeTasks)
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

                # If the Globus monitor isn't even aware of this transfer, try to notify it!
                elif globusStatus == "NOT FOUND":
                    notifyMonitorOfNewTransfer(globusID, task['contents'])

            # Reset timer to check NCSA api for transfer updates again
            apiWait = config['ncsa_api']['api_check_frequency_secs']
            writeTasksToDisk(config["status_log_path"], getStatus())

        # Refresh Globus auth tokens
        if authWait <= 0:
            generateAuthToken()
            authWait = config['globus']['authentication_refresh_frequency_secs']

"""Continually monitor FTP log for new files to transmit and add them to pendingTransfers"""
def ftpMonitorLoop():
    gantryWait = config['gantry']['file_check_frequency_secs'] # look for new files to send

    while True:
        time.sleep(1)
        gantryWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if gantryWait <= 0:
            pendingTransfers.update(getGantryFilesForTransfer(config['gantry']['incoming_files_path']))

            # Clean up the pending object of straggling keys, then initialize Globus transfer
            cleanPendingTransfers()
            if pendingTransfers != {}:
                writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)

            # Reset wait to check gantry incoming directory again
            gantryWait = config['gantry']['file_check_frequency_secs']
            writeTasksToDisk(config["status_log_path"], getStatus())

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

    # TODO: How to handle big errors, e.g. NCSA API not responding? admin email notification?

    # Load any previous active/pending transfers
    activeTasks = loadTasksFromDisk(config['active_tasks_path'])
    pendingTransfers = loadTasksFromDisk(config['pending_transfers_path'])

    # Create thread for service to begin monitoring log file & transfer queue
    log("*** Service now monitoring gantry transfer queue ***")
    thread.start_new_thread(globusMonitorLoop, ())
    log("*** Service now monitoring FTP log file ***")
    thread.start_new_thread(ftpMonitorLoop, ())

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    log("*** API now listening on "+config['api']['ip_address']+":"+apiPort+" ***")
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

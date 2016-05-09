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

rootPath = "/home/gantry"

config = {}

# Used by the FTP log reader to track progress
status_lastFTPLogLine = ""
status_numPending = 0
status_numActive = 0

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
def openFileToAppend(fpath=None):
    if not fpath:
        fpath = config["log_path"]

    # Create directories if necessary
    dirs = fpath.replace(os.path.basename(fpath), "")
    if not os.path.exists(dirs):
        os.makedirs(dirs)

    # Determine today's date (log_YYYYMMDD.txt)
    currD = time.strftime("%Y%m%d")
    fpath = fpath.replace(".txt", "_"+currD+".txt")

    # If there's a current log file, store it as log1.txt, log2.txt, etc.
    if os.path.exists(fpath):
        backupLog = fpath.replace(".txt", "_backup.txt")
        shutil.copyfile(fpath, backupLog)

    return open(fpath, 'a+')

"""Attempt to lock a file so API and monitor don't write at once, and wait if unable"""
def lockFile(f):
    # From http://tilde.town/~cristo/file-locking-in-python.html
    while True:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except {BlockingIOError, IOError} as e:
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
    logFile = openFileToAppend()
    logFile.write("["+type+"] "+message+"\n")
    logFile.close()

"""Return small JSON object with information about monitor health"""
def getStatus():
    global status_lastFTPLogLine
    global status_numActive
    global status_numPending

    completedTasks = 0
    for root, dirs, filelist in os.walk(config['completed_tasks_path']):
        completedTasks += len(filelist)

    return {
        "pending_file_transfers": status_numPending,
        "active_globus_tasks": status_numActive,
        "completed_globus_tasks": completedTasks,
        "last_ftp_log_line_read": status_lastFTPLogLine
    }

"""Load contents of .json file into a JSON object"""
def loadJsonFile(filename, markInaccessible=False):
    try:
        f = open(filename)
        jsonObj = json.load(f)
        f.close()
        return jsonObj
    except IOError:
        if not markInaccessible:
            log("unable to open "+filename+", returning {}")
        else:
            # Write filename to inaccessible list
            iaf = openFileToAppend(config['inaccessible_files_path'])
            iaf.write(filename+"\n")
            iaf.close()
            return False
        return {}

"""Load active or pending tasks from file into memory"""
def loadTasksFromDisk(filePath, emptyVal="{}"):
    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(filePath):
        if os.path.exists(filePath+".backup"):
            log("...loading tasks from "+filePath+".backup")
            shutil.copyfile(filePath+".backup", filePath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(filePath, 'w')
            f.write(emptyVal)
            f.close()
    else:
        log("...loading tasks from "+filePath)

    return loadJsonFile(filePath)

"""Write active or pending tasks from memory into file"""
def writeTasksToDisk(filePath, taskObj):
    # Write current file to backup location before writing current file
    log("...writing "+os.path.basename(filePath))

    # Create directories if necessary
    dirs = filePath.replace(os.path.basename(filePath), "")
    if not os.path.exists(dirs):
        os.makedirs(dirs)

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

    # Create path if necessary
    logPath = os.path.join(completedPath, taskID[:2], taskID[2:4], taskID[4:6], taskID[6:8])
    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # Write to json file with task ID as filename
    dest = os.path.join(logPath, taskID+".json")
    log("...complete: "+dest)
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

"""Create symlink to src file in destPath"""
def createLocalSymlink(srcPath, destPath, filename):
    try:
        if not os.path.isdir(destPath):
            os.makedirs(destPath)

        log("...creating symlink to "+filename+" in "+destPath)
        os.symlink(os.path.join(srcPath, filename),
                   os.path.join(destPath, filename))
    except OSError:
        #log("Unable to create dirs for "+destPath, "ERROR")
        return

"""Clear out any datasets from pendingTransfers without files or metadata"""
def cleanPendingTransfers():
    global pendingTransfers
    
    # Iterate across a copy since we'll be changing object
    allPendingTransfers = copy.deepcopy(pendingTransfers)
    for ds in allPendingTransfers:
        dsobj = allPendingTransfers[ds]
        if 'files' in dsobj and len(dsobj['files']) == 0:
            del pendingTransfers[ds]['files']
        if 'md' in dsobj and len(dsobj['md']) == 0:
            del pendingTransfers[ds]['md']
            if 'md_path' in dsobj:
                del pendingTransfers[ds]['md_path']
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

"""Add a particular file to pendingTransfers, checking for metadata first (used by manual API submissions)"""
def addFileToPendingTransfers(f):
    global pendingTransfers

    if f.find(config['gantry']['incoming_files_path']) > -1:
        gantryDirPath = f.replace(config['gantry']['incoming_files_path'], "")
    else:
        gantryDirPath = f.replace(config['globus']['source_path'], "")

    pathParts = gantryDirPath.split("/")
    filename = pathParts[-1]
    sensorname = pathParts[-4] if len(pathParts)>3 else "unknown_sensor"
    timestamp = pathParts[-2]  if len(pathParts)>1 else "unknown_time"
    datasetID = sensorname +" - "+timestamp
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
        mdobj = loadJsonFile(f, True)
        if mdobj:
            pendingTransfers = updateNestedDict(pendingTransfers, {
                datasetID: {
                    "md": mdobj,
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

    l = line.split()
    if len(l) > 6:
        YY = int(l[4])      # year, e.g. 2016
        MM = months[l[1]]   # month, e.g. 4
        DD = int(l[2])      # day of month, e.g. 5
        hh = int(l[3].split(':')[0])  # hours, e.g. 12
        mm = int(l[3].split(':')[1])  # minutes, e.g. 35
        ss = int(l[3].split(':')[2])  # seconds, e.g. 58

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

"""Refresh auth token and send autoactivate message to source and destination Globus endpoints"""
def activateEndpoints():
    src = config['globus']["source_endpoint_id"]
    dest = config['globus']["destination_endpoint_id"]

    generateAuthToken()
    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
    api.endpoint_autoactivate(src)
    api.endpoint_autoactivate(dest)

"""Generate a submission ID that can be used to avoid double-submitting"""
def generateGlobusSubmissionID():
    try:
        api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
        status_code, status_message, submission_id = api.submission_id()
    except (APIError, ClientError) as e:
        try:
            # Try activating endpoints and retrying
            activateEndpoints()
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
def getGantryFilesForTransfer():
    global status_numPending
    transferQueue = {}
    foundFiles = []

    gantryDir = config['gantry']['incoming_files_path']
    dockerDir = config['globus']['source_path']
    maxPending = config["gantry"]["max_pending_files"]

    # Get list of files from FTP log, if log is specified
    if status_numPending < maxPending:
        foundFiles = getNewFilesFromFTPLogs()

    # Get list of files from watched folders, if folders are specified  (and de-duplicate from FTP list)
    if status_numPending+len(foundFiles) < maxPending:
        fileList = getNewFilesFromWatchedFolders()
        for found in fileList:
            if found not in foundFiles:
                foundFiles.append(found)

    inaccessibleCount = 0
    for f in foundFiles:
        # Get dataset info & path details from found file
        if f.find(gantryDir) > -1:
            gantryDirPath = f.replace(gantryDir, "")
        else:
            gantryDirPath = f.replace(dockerDir, "")

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
                mdobj = loadJsonFile(f, True)
                if mdobj:
                    # Found metadata.json, assume it is for dataset
                    transferQueue[datasetID]['md'] = loadJsonFile(f)
                    transferQueue[datasetID]['md_path'] = gantryDirPath[1:] if gantryDirPath[0 ]== "/" else gantryDirPath
                else:
                    # File was inaccessible; remove from pending becuase it was logged in appropriate file
                    inaccessibleCount += 1

    status_numPending += len(foundFiles)
    status_numPending -= inaccessibleCount
    return transferQueue

"""Check folders in config for files older than the configured age, and queue for transfer"""
def getNewFilesFromWatchedFolders():
    global status_numPending
    foundFiles = []

    # Get list of files last modified more than X minutes ago
    watchDirs = config['gantry']['file_age_monitor_paths']
    fileAge = config['gantry']['min_file_age_for_transfer_mins']
    maxPending = config["gantry"]["max_pending_files"]

    for currDir in watchDirs:
        # TODO: Check for hidden files beginning with "." or just allow them?
        foundList = subprocess.check_output(["find", currDir, "-mmin", "+"+fileAge, "-type", "f", "-print"]).split("\n")
        if len(foundList)+len(foundFiles)+status_numPending > maxPending:
            break
        else:
            for f in foundList:
                foundFiles.append(f)

    status_numPending += len(foundFiles)
    return foundFiles

"""Check FTP log files to determine new files that were successfully moved to staging area"""
def getNewFilesFromFTPLogs():
    global status_lastFTPLogLine
    global status_numPending

    current_lastFTPLogLine = ""
    maxPending = config["gantry"]["max_pending_files"]
    foundFiles = []

    log("checking log files starting from >> "+status_lastFTPLogLine)
    logDir = config["gantry"]["ftp_log_path"]
    if logDir == "":
        # Don't perform a scan if no log file is defined
        return foundFiles

    # xferlog archived files are by date, e.g. "xferlog-20160501", "xferlog-20160502" - find these
    def isOldXferLog(fname):
        return fname.find("xferlog-") > -1
    lognames = filter(isOldXferLog, os.listdir(logDir))
    lognames.sort()

    # Example log line:
    #Tue Apr  5 12:35:58 2016 1 ::ffff:150.135.84.81 4061858 /gantry_data/LemnaTec/EnvironmentLogger/2016-04-05/2016-04-05_12-34-58_enviromentlogger.json b _ i r lemnatec ftp 0 * c
    lastLine = copy.copy(status_lastFTPLogLine)
    lastReadTime = parseDateFromFTPLogLine(lastLine)

    currLog = os.path.join(logDir, "xferlog")
    backLog = 0

    foundResumePoint = False
    handledBackLog = True
    while not (foundResumePoint and handledBackLog):
        with (open(currLog, 'r+') if currLog.find(".gz")==-1 else gzip.open(currLog, 'r+')) as f:
            log("scanning "+currLog)
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
                    log("found the resume point")
                    foundResumePoint = True

                elif foundResumePoint:
                    current_lastFTPLogLine = line

                    # We're past the last scanned line, so capture these lines if complete & ending in 'c'
                    if re.search('ftp \d \* c', line.rstrip()):
                        # Extract filename from log entry, after an IP address and a number (byte count?)
                        fnameRegex = '::ffff:\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3} \d+ ((\/?.)+) +\w _'
                        fname = re.search(fnameRegex, line)
                        if fname:
                            fullname = fname.group(1).rstrip()
                            fullname = fullname.replace(config['globus']['source_path'],"")
                            foundFiles.append(fullname)

                if len(foundFiles)+status_numPending >= maxPending:
                    break

        # If we didn't find last line in this file, look into the previous file
        if not foundResumePoint:
            handledBackLog = False
            backLog += 1

            if abs(backLog) > len(lognames):
                # No previous logs, so just start with current one
                currLog = os.path.join(logDir, "xferlog")
                foundResumePoint = True
                backLog = 0
            else:
                currLog = os.path.join(logDir, lognames[-backLog])

            log("walking back to "+currLog)

        # If we filled up the pending queue handling backlog, don't move onto newer files yet
        elif len(foundFiles)+status_numPending >= maxPending:
            handledBackLog = True

        # If we found last line in a previous file, climb back up to current file and get its contents too
        elif backLog > 0:
            backLog -= 1
            if backLog != 0:
                currLogName = lognames[-backLog]
                #currLogName = "xferlog-"+str(backLog) if backLog > 0 else "xferlog"
            else:
                currLogName = "xferlog"
            currLog = os.path.join(logDir, currLogName)
            log("walking up to "+currLog)

        # If we found the line and handled all backlogged files, we're ready to go
        else:
            handledBackLog = True

    log("queued "+str(len(foundFiles))+" files from log")
    status_lastFTPLogLine = current_lastFTPLogLine
    return foundFiles

"""Initiate Globus transfer with batch of files and add to activeTasks - recurse until max xfers reached or pending empty """
def initializeGlobusTransfer():
    global pendingTransfers
    global activeTasks
    global status_numActive
    global status_numPending

    maxQueue = config['globus']['max_transfer_file_count']

    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
    submissionID = generateGlobusSubmissionID()

    if submissionID:
        # Prepare transfer object
        transferObj = Transfer(submissionID,
                               config['globus']['source_endpoint_id'],
                               config['globus']['destination_endpoint_id'],
                               verify_checksum=True,
                               preserve_timestamp=True)

        queueLength = 0
        # Metadata is handled slightly differently as they aren't files adding to the Globus transfer, but they ARE
        # metadata.json files in the pending files queue that need to be purged correctly after the transfer starts.
        mdQueueLength = 0
        sentSomeMd = False

        # Loop over a copy of the list instead of actual thing - other thread will be appending to actual thing
        loopingTransfers = copy.deepcopy(pendingTransfers)
        # This will hold the leftover pending transfers once we've hit the max size for this transfer
        remainingPendingTransfers = copy.deepcopy(pendingTransfers)
        currentTransferBatch = {}

        for ds in loopingTransfers:
            if "files" in loopingTransfers[ds]:
                if ds not in currentTransferBatch:
                    currentTransferBatch[ds] = {}
                    currentTransferBatch[ds]['files'] = {}
                # Add files from each dataset
                for f in loopingTransfers[ds]['files']:
                    if queueLength < maxQueue:
                        fobj = loopingTransfers[ds]['files'][f]
                        src_path = os.path.join(config['globus']['source_path'], fobj["path"], fobj["name"])
                        dest_path = os.path.join(config['globus']['destination_path'], fobj["path"],  fobj["name"])
                        transferObj.add_item(src_path, dest_path)

                        # remainingTransfers will have leftover data once max Globus transfer size is met
                        queueLength += 1
                        currentTransferBatch[ds]['files'][f] = fobj
                        del remainingPendingTransfers[ds]['files'][f]
                    else:
                        break
                if "md" in loopingTransfers[ds]:
                    mdQueueLength += 1
                    currentTransferBatch[ds]['md'] = loopingTransfers[ds]['md']
                    del remainingPendingTransfers[ds]['md']
                    if "md_path" in loopingTransfers[ds]:
                        del remainingPendingTransfers[ds]['md_path']

                # Clean up placeholder entries once queue length is exceeded
                if currentTransferBatch[ds]['files'] == {}:
                    del currentTransferBatch[ds]['files']
                if currentTransferBatch[ds] == {}:
                    del currentTransferBatch[ds]

            elif "md" in loopingTransfers[ds]:
                # We have metadata for a dataset, but no files. Just send metadata separately.
                mdxfer = sendMetadataToMonitor(ds, loopingTransfers[ds]['md'])
                # Leave metadata in pending if it wasn't successfully posted to Clowder
                if mdxfer.status_code == 200:
                    sentSomeMd = True
                    # Otherwise remove dataset entry since we already know there are no files
                    status_numPending -= 1
                    del remainingPendingTransfers[ds]
                    # Move .json file to deletion queue
                    createLocalSymlink(os.path.join(config['gantry']['incoming_files_path'], loopingTransfers[ds]['md_path']),
                                  os.path.join(config['gantry']['deletion_queue'], loopingTransfers[ds]['md_path']), "metadata.json")

            if queueLength >= maxQueue:
                break

        if queueLength > 0:
            # Send transfer to Globus
            try:
                status_code, status_message, transfer_data = api.transfer(transferObj)
            except (APIError, ClientError) as e:
                try:
                    # Try refreshing endpoints and retrying
                    activateEndpoints()
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
                status_numPending -= queueLength
                status_numPending -= mdQueueLength
                writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)
            else:
                # If failed, leave pending list as-is and try again on next iteration (e.g. in 180 seconds)
                log("globus transfer failed for "+ds+" ("+str(status_code)+": "+str(status_message)+")", "ERROR")
                return
        elif sentSomeMd:
            # If metadata was sent there was still activity, so update pending transfers
            pendingTransfers = remainingPendingTransfers
            writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)

        status_numActive = len(activeTasks)
        cleanPendingTransfers()
        if pendingTransfers != {} and status_numActive < config['globus']['max_active_tasks']:
            # If pendingTransfers not empty, we still have remaining files and need to start more Globus transfers
            initializeGlobusTransfer()

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
    global activeTasks
    global status_numActive

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
            if status_numActive < config["globus"]["max_active_tasks"]:
                log("initializing Globus transfers")
                # Clean up the pending object of straggling keys, then initialize Globus transfers
                cleanPendingTransfers()
                if pendingTransfers != {}:
                    writeTasksToDisk(config['pending_transfers_path'], pendingTransfers)
                    initializeGlobusTransfer()

            # Reset wait to check gantry incoming directory again
            globusWait = config['gantry']['globus_transfer_frequency_secs']
            writeTasksToDisk(config["status_log_path"], getStatus())

        # Check with NCSA Globus monitor API for completed transfers
        if apiWait <= 0:
            log("checking status of active transfers with NCSA")
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
                                        createLocalSymlink(os.path.join(config['gantry']['incoming_files_path'], fobj['path']),
                                                      os.path.join(deleteDir, fobj['path']), fobj['name'])
                                        if 'md'in fobj:
                                            createLocalSymlink(os.path.join(config['gantry']['incoming_files_path'], fobj['md_path']),
                                                          os.path.join(deleteDir, fobj['md_path']), fobj['md_name'])
                                if 'md' in task['contents'][ds]:
                                    dsobj = task['contents'][ds]
                                    createLocalSymlink(os.path.join(config['gantry']['incoming_files_path'], dsobj['md_path']),
                                                  os.path.join(deleteDir, dsobj['md_path']), "metadata.json")

                            # Crawl and remove empty directories
                            # log("...removing empty directories in "+config['gantry']['incoming_files_path'])
                            # subprocess.call(["find", config['gantry']['incoming_files_path'], "-type", "d", "-empty", "-delete"])

                    del activeTasks[globusID]
                    writeTasksToDisk(config['active_tasks_path'], activeTasks)

                # If the Globus monitor isn't even aware of this transfer, try to notify it!
                elif globusStatus == "NOT FOUND":
                    notifyMonitorOfNewTransfer(globusID, task['contents'])

            status_numActive = len(activeTasks)

            # Reset timer to check NCSA api for transfer updates again
            apiWait = config['ncsa_api']['api_check_frequency_secs']
            writeTasksToDisk(config["status_log_path"], getStatus())

        # Refresh Globus auth tokens
        if authWait <= 0:
            generateAuthToken()
            authWait = config['globus']['authentication_refresh_frequency_secs']

"""Continually monitor FTP log for new files to transmit and add them to pendingTransfers"""
def gantryMonitorLoop():
    gantryWait = 1 #config['gantry']['file_check_frequency_secs'] # look for new files to send

    while True:
        time.sleep(1)
        gantryWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if gantryWait <= 0:
            if status_numPending < config["gantry"]["max_pending_files"]:
                pendingTransfers.update(getGantryFilesForTransfer())

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
    activateEndpoints()

    # TODO: How to handle big errors, e.g. NCSA API not responding? admin email notification?

    # Get last read log line from previous run
    if os.path.exists(config["status_log_path"]):
        monitorData = loadJsonFile(config["status_log_path"])
        status_lastFTPLogLine = monitorData["last_ftp_log_line_read"]

    # Load any previous active/pending transfers
    activeTasks = loadTasksFromDisk(config['active_tasks_path'])
    status_numActive = len(activeTasks)
    pendingTransfers = loadTasksFromDisk(config['pending_transfers_path'])
    cleanPendingTransfers()
    for ds in pendingTransfers:
        if 'files' in pendingTransfers[ds]:
            for f in pendingTransfers[ds]['files']:
                status_numPending += 1

    log("loaded data from active and pending log files")
    log(str(status_numPending)+" pending files")
    log(str(status_numActive)+" active Globus tasks")

    # Create thread for service to begin monitoring log file & transfer queue
    log("*** Service now monitoring gantry transfer queue ***")
    thread.start_new_thread(globusMonitorLoop, ())
    log("*** Service now checking for new files via FTP logs/folder monitoring ***")
    thread.start_new_thread(gantryMonitorLoop, ())

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    log("*** API now listening on "+config['api']['ip_address']+":"+apiPort+" ***")
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

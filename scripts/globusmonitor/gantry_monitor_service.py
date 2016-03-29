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

import os, shutil, json, time, datetime, thread, copy, subprocess
import requests
from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, goauth


config = {}
configFile = "config_gantry.json"

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
    "files":        [{          list of files included in task, each with
            "path": "file1",        ...file path, which is updated with path-on-disk once completed
            "md": {},               ...metadata to be associated with that file
            "dataset":              ...dataset file belongs to
        }, {...}, ...],
    "md":                       metadata to be associated with dataset
    "started":                  timestamp when task was sent to Globus
    "completed":                timestamp when task was completed (including errors and cancelled tasks)
    "status":                   can be "IN PROGRESS", "DONE", "ABORTED", "ERROR"
}, {...}, {...}, ...}"""
activeTasks = {}

# ----------------------------------------------------------
# SHARED UTILS
# ----------------------------------------------------------
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
            print("...loading tasks from "+filePath+".backup")
            shutil.copyfile(filePath+".backup", filePath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(filePath, 'w')
            f.write("{}")
            f.close()
    else:
        print("...loading tasks from "+filePath)

    return loadJsonFile(filePath)

"""Write active or pending tasks from memory into file"""
def writeTasksToDisk(filePath, taskObj):
    # Write current file to backup location before writing current file
    print("...writing tasks to "+filePath)

    if os.path.exists(filePath):
        shutil.move(filePath, filePath+".backup")

    f = open(filePath, 'w')
    f.write(json.dumps(taskObj))
    f.close()

"""Write a completed task onto disk in appropriate folder hierarchy"""
def writeCompletedTransferToDisk(transfer):
    completedPath = config['gantry']['completed_path']
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
    print("...writing completed transfer to "+dest)
    f = open(dest, 'w')
    f.write(json.dumps(transfer))
    f.close()

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access token for config account"""
def generateAuthToken():
    print("...generating auth token for "+config['globus']['username'])
    config['globus']['auth_token'] = goauth.get_access_token(
            username=config['globus']['username'],
            password=config['globus']['password']
        ).token

"""Generate a submission ID that can be used to avoid double-submitting"""
def generateGlobusSubmissionID():
    try:
        api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
        status_code, status_message, submission_id = api.submission_id()
    except APIError:
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

    for root, dirs, files in os.walk(gantryDir):
        # gantryDir/sensorname/YYYY-MM-DD/YYMMDD_HHMMSS/
        #       datasetID = "sensorname YYMMDD_HHMMSS"
        pathParts = root.split("/")
        if len(pathParts) >= 3:
            datasetID = pathParts[-3]+" "+pathParts[-1]
        else:
            datasetID = pathParts[-1]

        # TODO: Implement simple API that will let MAC script tell us which files are done
        foundFiles = subprocess.check_output(["find", root, "-mmin", "+15", "-type", "f", "-print"]).split("\n")

        for f in foundFiles:
            filename = f.replace(root,"")
            # Check whether file is already queued for transfer
            if f != "" and filename not in pendingTransfers[datasetID]['files']:
                if filename.find("metadata") == -1 and filename[0] != ".":
                    # TODO: Check for .json file with same name as current file - assume metadata if so
                    transferQueue[datasetID]['files'][filename] = {
                        "name": filename,
                        "path": root,
                        "dataset": datasetID
                        # "md": {}, only include md key if present
                        # "md_name": "" metadata filename
                        # "md_path": "" metadata folder
                    }
                elif filename.find("metadata") > -1:
                    # Found a metadata file, assume it is for dataset
                    transferQueue[datasetID]['md'] = loadJsonFile(f)
                    transferQueue[datasetID]['md_path'] = root

    return transferQueue

"""Initiate Globus transfer with batch of files and add to activeTasks"""
def initializeGlobusTransfers():
    submissionID = generateGlobusSubmissionID()

    # Prepare transfer object
    transferObj = Transfer(submissionID,
                           config['globus']['source_endpoint_id'],
                           config['globus']['destination_endpoint_id'])

    for ds in pendingTransfers:
        if "files" in pendingTransfers[ds]:
            # Add files from each dataset
            for f in pendingTransfers[ds]['files']:
                src_path = os.path.join(config['gantry']['incoming_files_path'], f["name"])
                dest_path = os.path.join(config['globus']['destination_path'], f["name"])
                transferObj.add_item(src_path, dest_path)
        elif "md" in pendingTransfers[ds]:
            # We have metadata for a dataset, but no files. Just send metadata separately.
            sendMetadataToMonitor(ds, pendingTransfers[ds]['md'])

    # Send transfer to Globus
    try:
        api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
        status_code, status_message, transfer_data = api.transfer(transferObj)
    except APIError:
        # Try refreshing auth token and retrying
        generateAuthToken()
        api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
        status_code, status_message, transfer_data = api.transfer(transferObj)

    # Notify NCSA monitor of new task, and add to activeTasks for logging
    if status_code == 200 or status_code == 202:
        globusID = transfer_data['task_id']

        # TODO: Harden this a bit - what happens if we crash at various points?
        activeTasks[globusID] = {
            "globus_id": globusID,
            "files": pendingTransfers,
            "started": str(datetime.datetime.now()),
            "status": "IN PROGRESS"
        }
        writeTasksToDisk(config['gantry']['active_path'], activeTasks)

        notifyMonitorOfNewTransfer(globusID, pendingTransfers)

        pendingTransfers = {}
        writeTasksToDisk(config['gantry']['pending_path'], pendingTransfers)
    else:
        print("[ERROR] globus initialization failed for "+ds+" ("+status_code+": "+status_message+")")


"""Send message to NCSA Globus monitor API that a new task has begun"""
def notifyMonitorOfNewTransfer(globusID, contents):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    sess.post(config['api']['host']+"/tasks", data=json.dumps({
        "user": config['globus']['username'],
        "globus_id": globusID,
        "contents": contents
    }))

"""Send message to NCSA Globus monitor API with metadata for a dataset, without other files"""
def sendMetadataToMonitor(datasetName, metadata):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    # Check with Globus monitor rather than Globus itself, to make sure file was handled properly before deleting from src
    status = sess.post(config['api']['host']+"/metadata", data=json.dumps({
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
    status = sess.get(config['api']['host']+"/tasks/"+globusID, headers={"Content-Type": "application/json"})

    return status

"""Continually monitor gantry directory for new files to transmit"""
def gantryMonitorLoop():
    # Prepare timers for tracking how often different refreshes are executed
    gantryWait = config['gantry']['file_check_frequency'] # look for new files to send
    apiWait = config['api']['api_check_frequency'] # check status of sent files
    authWait = config['api']['authentication_refresh_frequency_secs'] # renew globus auth

    while True:
        time.sleep(1)
        gantryWait -= 1
        apiWait -= 1
        authWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if gantryWait <= 0:
            pendingTransfers.update(
                    getGantryFilesForTransfer(config['gantry']['incoming_files_path']))
            writeTasksToDisk(config['gantry']['pending_path'], pendingTransfers)
            initializeGlobusTransfers()
            # Reset wait to check gantry incoming directory again
            gantryWait = config['gantry']['file_check_frequency']

        # Check with NCSA Globus monitor API for completed transfers
        if apiWait <= 0:
            # Use copy of task list so it doesn't change during iteration
            currentActiveTasks = copy.copy(activeTasks)
            for globusID in currentActiveTasks:
                task = activeTasks[globusID]

                globusStatus = getTransferStatusFromMonitor(globusID)
                if globusStatus in ["SUCCEEDED", "FAILED"]:
                    print("[TASK] status update for "+globusID+": "+globusStatus)
                    task['status'] = globusStatus
                    task['completed'] = str(datetime.datetime.now())

                    # Write out results log
                    writeCompletedTransferToDisk(task)

                    # Move files (and metadata files if needed) to staging area for deletion
                    if globusStatus == "SUCCEEDED":
                        deleteDir = config['gantry']['deletion_queue']
                        if 'files' in task:
                            for f in task['files']:
                                shutil.move(os.path.join(f['path'], f['name']),
                                            os.path.join(deleteDir, f['name']))
                                if 'md'in f:
                                    shutil.move(os.path.join(f['md_path'], f['md_name']),
                                                os.path.join(deleteDir, f['md_name']),)
                        if 'md' in task:
                            shutil.move(os.path.join(f['md_path'], "metadata.json"),
                                        os.path.join(deleteDir, "metadata.json"),)

                        # TODO: Crawl and remove empty directories?

                    del activeTasks[globusID]
                    writeTasksToDisk(config['gantry']['active_path'], activeTasks)

            # Reset timer to check NCSA api for transfer updates again
            apiWait = config['api']['api_check_frequency']

        # Refresh Globus auth tokens
        if authWait <= 0:
            generateAuthToken()
            authWait = config['api']['authentication_refresh_frequency_secs']


if __name__ == '__main__':
    print("...loading configuration from "+configFile)
    config = loadJsonFile(configFile)
    generateAuthToken()

    # Load any previous active/pending transfers
    activeTasks = loadTasksFromDisk(config['gantry']['active_path'])
    pendingTransfers = loadTasksFromDisk(config['gantry']['pending_path'])

    # Create thread for service to begin monitoring
    print("*** Service now monitoring gantry directory ***")
    gantryMonitorLoop()

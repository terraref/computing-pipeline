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

import os, shutil, json, time, datetime, thread, copy
import requests
from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, goauth


config = {}
configFile = "config_gantry.json"

"""pendingTransfers tracks individual files and is of the format:
{"filename": {
    "name":         filename
    "md": {}        metadata object to associate with file
}, {...} ...}"""
pendingTransfers = {}

"""activeTasks tracks Globus transfers is of the format:
{"globus_id": {
    "globus_id":                globus job ID of upload
    "files":        [{          list of files included in task, each with
            "path": "file1",        ...file path, which is updated with path-on-disk once completed
            "md": {}                ...metadata to be associated with that file
        }, {...}, ...],
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

"""Load activeTasks from file into memory"""
def loadActiveTasksFromDisk():
    activePath = config['gantry']['active_path']

    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(activePath):
        if os.path.exists(activePath+".backup"):
            print("...loading active tasks from "+activePath+".backup")
            shutil.copyfile(activePath+".backup", activePath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(activePath, 'w')
            f.write("{}")
            f.close()
    else:
        print("...loading active tasks from "+activePath)

    activeTasks = loadJsonFile(activePath)

"""Write activeTasks from memory into file"""
def writeActiveTasksToDisk():
    # Write current file to backup location before writing current file
    activePath = config['gantry']['active_path']
    print("...writing active tasks to "+activePath)

    if os.path.exists(activePath):
        shutil.move(activePath, activePath+".backup")

    f = open(activePath, 'w')
    f.write(json.dumps(activeTasks))
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

"""Return full path to completed logfile for a given task id if it exists, otherwise None"""
def getCompletedTransferLogPath(taskID):
    completedPath = config['gantry']['completed_path']

    treeLv1 = os.path.join(completedPath, taskID[:2])
    treeLv2 = os.path.join(treeLv1, taskID[2:4])
    treeLv3 = os.path.join(treeLv2, taskID[4:6])
    treeLv4 = os.path.join(treeLv3, taskID[6:8])
    fullPath = os.path.join(treeLv4, taskID+".json")

    return fullPath

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
    foundFiles = [] # find gantryDir -mmin +15 -type f -print

    # Check whether file is already queued for transfer
    newReadyFiles = {}
    for f in foundFiles:
        if (f not in pendingTransfers) and (f not in activeTasks):
            newReadyFiles[f] = {
                "name": f,
                "md": {}
            }

    return newReadyFiles

"""Initiate Globus transfer with batch of files and add to activeTasks"""
def initializeGlobusTransfers():
    submissionID = generateGlobusSubmissionID()

    # TODO: How should metadata be determined, and how should files be bundled? Do we need to evaluate contents of pendingTransfers?

    # Prepare transfer object
    transferObj = Transfer(submissionID,
                           config['globus']['source_endpoint_id'],
                           config['globus']['destination_endpoint_id'])
    for f in pendingTransfers:
        src_path = os.path.join(config['gantry']['incoming_files_path'], f["name"])
        dest_path = os.path.join(config['globus']['destination_path'], f["name"]))
        transferObj.add_item(src_path, dest_path)

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
        notifyMonitorOfNewTransfer(globusID, pendingTransfers)

        activeTasks[globusID] = {
            "globus_id": globusID,
            "files": pendingTransfers,
            "started": str(datetime.datetime.now()),
            "status": "IN PROGRESS"
        }
        writeActiveTasksToDisk()

        pendingTransfers = {}
        return globusID
    else:
        return "UNKNOWN ("+status_code+": "+status_message+")"

"""Send message to NCSA Globus monitor API that a new task has begun"""
def notifyMonitorOfNewTransfer(globusID, fileObj):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    sess.post(config['api']['host']+"/tasks", data=json.dumps({
        "user": config['globus']['username'],
        "globus_id": globusID,
        "files": fileObj
    }))

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
    gantryWait = config['gantry']['file_check_frequency']
    apiWait = config['api']['api_check_frequency']
    authWait = config['api']['authentication_refresh_frequency_secs']

    while True:
        time.sleep(1)
        gantryWait -= 1
        apiWait -= 1
        authWait -= 1

        # Check for new files in incoming gantry directory and initiate transfer
        if gantryWait <= 0:
            gantryDir = config['gantry']['incoming_files_path']

            # Get list of files that are ready to send
            pendingTransfers.update(getGantryFilesForTransfer(gantryDir))
            initializeGlobusTransfers()

            # Reset timer
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

                    # TODO: Flag file for deletion if transfer successful
                    if globusStatus == "SUCCEEDED":
                        pass

                    # Write out results file, then delete from active list and write new active file
                    writeCompletedTransferToDisk(task)
                    del activeTasks[globusID]
                    writeActiveTasksToDisk()

            # Reset timer
            apiWait = config['api']['api_check_frequency']

        # Refresh Globus auth tokens
        if authWait <= 0:
            generateAuthToken()

            # Reset timer
            authWait = config['api']['authentication_refresh_frequency_secs']


if __name__ == '__main__':
    print("...loading configuration from "+configFile)
    config = loadJsonFile(configFile)
    loadActiveTasksFromDisk()
    generateAuthToken()

    # Create thread for service to begin monitoring
    print("*** Service now monitoring gantry directory ***")
    gantryMonitorLoop()

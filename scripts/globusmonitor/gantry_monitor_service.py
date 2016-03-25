#!/usr/bin/python

# ----------------------------------------------------------
# GANTRY MONITOR SERVICE
# This will load parameters from the configFile defined below,
# and begin monitoring the specified gantry file directory for
# new files. When transfer from the gantry to the directory
# is complete, this service will initiate the Globus transfer
# and send notification to the API at the receiving end.
# ----------------------------------------------------------

import os, shutil, json, time, datetime, thread, copy
import requests
from globusonline.transfer.api_client import TransferAPIClient, goauth


config = {}
configFile = "config_gantry.json"

"""Pending task object is of the format:
{"filename": {
    "metadata": {},     metadata object to associate with file
    "globus_id":        globus ID of task, once transfer has begun
}
"""
pendingTransfers = {}

"""Active task object is of the format:
{"globus_id": {
    "user":                     globus username
    "globus_id":                globus job ID of upload
    "files":        [{          list of files included in task, each with
            "path": "file1",        ...file path, which is updated with path-on-disk once completed
            "md": {}                ...metadata to be associated with that file
        }, {...}, ...],
    "received":                 timestamp when task was sent to monitor API
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
def loadPendingTransfersFromDisk():
    pendingPath = config['api']['pendingPath']

    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(pendingPath):
        if os.path.exists(pendingPath+".backup"):
            print("...loading pending transfers from "+pendingPath+".backup")
            shutil.copyfile(pendingPath+".backup", pendingPath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(pendingPath, 'w')
            f.write("{}")
            f.close()
    else:
        print("...loading pending transfers from "+pendingPath)

    pendingTransfers = loadJsonFile(pendingPath)

"""Write activeTasks from memory into file"""
def writePendingTransfersToDisk():
    # Write current file to backup location before writing current file
    pendingPath = config['api']['pendingPath']
    print("...writing active tasks to "+pendingPath)

    if os.path.exists(pendingPath):
        shutil.move(pendingPath, pendingPath+".backup")

    f = open(pendingPath, 'w')
    f.write(json.dumps(pendingTransfers))
    f.close()

"""Write a completed task onto disk in appropriate folder hierarchy"""
def writeCompletedTransferToDisk(transfer):
    completedPath = config['api']['completedPath']
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
    completedPath = config['api']['completedPath']

    treeLv1 = os.path.join(completedPath, taskID[:2])
    treeLv2 = os.path.join(treeLv1, taskID[2:4])
    treeLv3 = os.path.join(treeLv2, taskID[4:6])
    treeLv4 = os.path.join(treeLv3, taskID[6:8])
    fullPath = os.path.join(treeLv4, taskID+".json")

    return fullPath

"""Return list of full paths to completed files sent in a particular task"""
def getCompletedTaskPathList(taskID):
    globusHome = config['globus']['homePath']

    outFiles = []
    for f in pendingTransfers[taskID]['files']:
        outFiles.append(os.path.join(globusHome, f))

    return outFiles

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access token for config account"""
def generateAuthToken():
    print("...generating auth token for "+config['globus']['username'])
    config['globus']['authToken'] = goauth.get_access_token(
            username=config['globus']['username'],
            password=config['globus']['password']
        ).token

"""Check for files ready for transmission and return list"""
def checkFileModifiedStatus(gantryDir):
    readyFiles = []
    # find gantryDir -mmin +15 -type f -print
    return readyFiles

"""Continually monitor gantry directory for new files to transmit"""
def gantryMonitorLoop():
    refreshTimer = 0
    while True:
        time.sleep(1)

        # Get list of files in directory with created time
        gantryDir = config['gantry']['homePath']
        readyFiles = checkFileModifiedStatus(gantryDir)

        for f in readyFiles:
            # Check whether f is already in JSON file (ready for transfer but pending)
            f in pendingTransfers

        for f in pendingTransfers:
            # Initiate transfer and add to activeTasks
            # TODO: How should transfers be batched together?
            initializeGlobusTransfer(f)

        for f in activeTasks:
            # Check w/ Globus monitor API and move completed files into deleteme directory
            pass

        # Use copy of task list so it doesn't change during iteration
        currentActiveTasks = copy.copy(activeTasks)
        for globusID in currentActiveTasks:
            # For in-progress tasks, check Globus for status updates
            task = activeTasks[globusID]
            readyFiles = checkFileModifiedStatus(task)

            if globusStatus in ["SUCCEEDED", "FAILED"]:
                print("[TASK] status update for "+globusID+": "+globusStatus)

                # Update task parameters
                task['status'] = globusStatus
                task['completed'] = str(datetime.datetime.now())
                task['path'] = getCompletedTransferLogPath(globusID)
                files = getCompletedTaskPathList(globusID)

                # Notify Clowder to process file if transfer successful
                if globusStatus == "SUCCEEDED":
                    initializeGlobusTransfer(task, files)

                # Write out results file, then delete from active list and write new active file
                writeCompletedTransferToDisk(task)
                del activeTasks[globusID]
                writePendingTransfersToDisk()

        # Refresh auth tokens every 12 hours
        refreshTimer += 1
        if refreshTimer >= 43200:
            generateAuthToken()
            refreshTimer = 0

"""Send Clowder necessary details to load local file after Globus transfer complete"""
def initializeGlobusTransfer(file):
    pass

if __name__ == '__main__':
    print("...loading configuration from "+configFile)
    config = loadJsonFile(configFile)
    loadPendingTransfersFromDisk()
    generateAuthToken()

    # Create thread for service to begin monitoring
    print("*** Service now monitoring gantry directory ***")
    gantryMonitorLoop()

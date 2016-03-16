#!/usr/bin/python

import os, json
from globusonline.transfer.api_client import TransferAPIClient, goauth

config = {}
activeJobs = {}
completed = {}
validUsers = []


"""Load config parameters from config.json file"""
def loadConfigFromFile():
    configFile = open("config.json")
    config = json.load(configFile)
    configFile.close()
    return config

"""Load list of accepted Globus users whose jobs can be monitored"""
def loadValidUsersFromFile():
    userListPath = config.api.userListPath
    if not os.path.exists(userListPath):
        # Create an empty file if it doesn't exist already
        usersFile = open(userListPath, 'w')
        usersFile.write("[]")
        usersFile.close()
    usersFile = open(userListPath)
    validUsers = json.load(usersFile)
    usersFile.close()

"""Load previously written details of in-progress and completed jobs from file"""
def loadJobListsFromFile():
    """
        Both in-progress and completed job files are of the format:
        [
            {
                "user": globus username,
                "globus_id": globus job ID of upload,
                "files": ["file1", "file2", ...]},
            {...},
            {...},
            ...
        ]
    """
    activeJobsPath = config.api.activeJobsPath
    if not os.path.exists(activeJobsPath):
        # Create an empty file if it doesn't exist already
        jobFile = open(activeJobsPath, 'w')
        jobFile.write("{}")
        jobFile.close()
    jobFile = open(activeJobsPath)
    activeJobs = json.load(jobFile)
    jobFile.close()

    resultsPath = config.api.resultsPath
    if not os.path.exists(resultsPath):
        # Create an empty file if it doesn't exist already
        doneFile = open(resultsPath, 'w')
        doneFile.write("{}")
        doneFile.close()
    doneFile = open(resultsPath)
    completed = json.load(doneFile)
    doneFile.close()

"""Write activeJobs & completed list from memory into file"""
def writeJobListsToFile():
    # We don't care what the current contents are; they should either already be loaded or are outdated. Overwrite 'em.
    activeJobsPath = config.api.activeJobsPath
    jobFile = open(activeJobsPath, 'w')
    jobFile.write(json.dumps(activeJobs))
    jobFile.close()

    resultsPath = config.resultsPath
    doneFile = open(resultsPath, 'w')
    doneFile.write(json.dumps(completed))
    doneFile.close()

"""Use globus goauth tool to get access token for account"""
def generateGoauthToken(username, password):
    authData = goauth.get_access_token(username=username, password=password)
    return authData.token

"""Query Globus API re: globusID to get current transfer status"""
def checkGlobusStatus(globusID):
    # Examples here: https://github.com/globusonline/transfer-api-client-python/tree/master/globusonline/transfer/api_client/examples
    api = TransferAPIClient(username=config.globus.username, goauth=authToken)
    status_code, status_message, data = api.task_list()

"""Send Clowder necessary details to load local file after Globus transfer complete"""
def notifyClowderOfCompletedFile(globusID):
    pass


# Main loop
config = loadConfigFromFile()
while True:
    activeJobs = getActiveJobsFromAPI()
    for gid in activeJobs.keys:
        status = checkGlobusStatus(gid)
        if status == "DONE":
            # tell clowder upload is finished
            completed[gid] = activeJobs[gid]
            del activeJobs[gid]
            writeJobListsToFile()
            notifyClowderOfCompletedFile(gid)
        elif status == "ERROR":
            # mark as error in log file
            completed.append(gid)
            del activeJobs[gid]

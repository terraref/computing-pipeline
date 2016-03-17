#!/usr/bin/python

import os, json, time
from flask import Flask, request
from flask.ext import restful
from flask_restful import reqparse, abort, Api, Resource
from globusonline.transfer.api_client import TransferAPIClient, goauth

config = {}
authToken = None

"""Both in-progress and completed job objects are of the format:
[
    {
        "user": globus username,
        "globus_id": globus job ID of upload,
        "files": ["file1", "file2", ...]
    },
    {...}, {...}, ...
]
"""
activeTasks = {}
completedTasks = {}

app = Flask(__name__)
api = restful.Api(app)

# ----------------------------------------------------------
# SHARED UTILS
# ----------------------------------------------------------
"""Load config parameters from config.json file"""
def loadConfigFromFile():
    configFile = open("config.json")
    config = json.load(configFile)
    configFile.close()

"""Load previously written details of in-progress and completed jobs from file"""
def loadJobListsFromFile():
    activePath = config.service.activePath
    if not os.path.exists(activePath):
        # Create an empty file if it doesn't exist already
        f = open(activePath, 'w')
        f.write("{}")
        f.close()
    f = open(activePath)
    activeTasks = json.load(f)
    f.close()

    completedPath = config.service.completedPath
    if not os.path.exists(completedPath):
        # Create an empty file if it doesn't exist already
        f = open(completedPath, 'w')
        f.write("{}")
        f.close()
    f = open(completedPath)
    completedTasks = json.load(f)
    f.close()

"""Write activeTasks & completedTasks from memory into file"""
def writeJobListsToFile():
    # We don't care what the current contents are; they should either already be loaded or are outdated. Overwrite 'em.
    activePath = config.service.activePath
    f = open(activePath, 'w')
    f.write(json.dumps(activeTasks))
    f.close()

    completedPath = config.service.completedPath
    f = open(completedPath, 'w')
    f.write(json.dumps(completedTasks))
    f.close()


# ----------------------------------------------------------
# API COMPONENTS
# ----------------------------------------------------------
"""API - Post new globus tasks to be monitored"""
class GlobusMonitor(restful.Resource):

    """Add new Globus task ID from a known user for monitoring"""
    def post(self):
        json_data = request.get_json(force=True)
        for task in json_data:
            globus_user = str(task['user'])
            globus_id = str(task['globus_id'])
            file_list = str(task['files'])

            # Check if globus username is known
            if globus_user in config.globus.validUsers.keys:
                # Add to local queue that is checked all the time and saved to disk
                activeTasks[globus_id] = {
                    "user": globus_user,
                    "files": file_list,
                    "status": "IN PROGRESS"
                }

        return 201

"""API - Get status of a particular task by globus id"""
class GlobusTask(restful.Resource):

    """Check if the Globus task ID is finished, in progress, or an error has occurred"""
    def get(self, globusID):
        if globusID not in activeTasks.keys and globusID not in completedTasks.keys:
            return "Globus ID not found", 404
        elif globusID not in activeTasks.keys:
            return completedTasks[globusID], 200
        else:
            return activeTasks[globusID], 200

# Add a new Globus id that should be monitored
api.add_resource(GlobusMonitor, '/upload')
# Check to see if Globus id is finished
api.add_resource(GlobusTask, '/upload/<string:globusID>')

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access token for account"""
def generateAuthToken(username, password):
    authData = goauth.get_access_token(username=username, password=password)
    return authData.token

"""Query Globus API re: globusID to get current transfer status"""
def checkGlobusStatus(globusID):
    if globusID in activeTasks.keys:
        taskUser = activeTasks[globusID].user
        taskToken = config.globus.validUsers[taskUser].authToken

        api = TransferAPIClient(username=taskUser, goauth=taskToken)
        status_code, status_message, data = api.task_list()
        for gid in data.DATA:
            # check if status has changed...
            status = data.DATA[gid].status

"""Continually check globus API for task updates"""
def globusMonitorLoop():
    while True:
        time.sleep(2)
        for gid in activeTasks.keys:
            status = checkGlobusStatus(gid)
            if status == "DONE":
                # tell clowder upload is finished
                completedTasks[gid] = activeTasks[gid]
                del activeTasks[gid]
                writeJobListsToFile()
                notifyClowderOfCompletedFile(gid)
            elif status == "ERROR":
                # mark as error in log file
                completedTasks[gid] = activeTasks[gid]
                completedTasks[gid].status = "ERROR"
                del activeTasks[gid]
                writeJobListsToFile()

"""Send Clowder necessary details to load local file after Globus transfer complete"""
def notifyClowderOfCompletedFile(globusID):
    pass


if __name__ == '__main__':
    loadConfigFromFile()

    # Create thread for API to begin listening
    thread.start_new_thread(app.run, kwargs={
        "host": "0.0.0.0",
        "port": int(config.api.port),
        "debug": True
    })
    print("API now listening on port "+config.api.port)

    # Generate auth tokens for primary & validated users
    #authToken = generateAuthToken(config.globus.username, config.globus.password) TODO: main globus creds not needed?
    for validUser in config.globus.validUsers.keys:
        config.globus.validUsers[validUser].authToken = generateAuthToken(validUser, config.globus.validUsers[validUser].password)

    # Create thread for service to begin monitoring
    thread.start_new_thread(globusMonitorLoop)
    print("Service now monitoring Globus tasks")

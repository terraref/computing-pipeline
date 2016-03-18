#!/usr/bin/python

import os, shutil, json, time, datetime
from flask import Flask, request
from flask.ext import restful
from flask_restful import reqparse, abort, Api, Resource
from globusonline.transfer.api_client import TransferAPIClient, goauth


config = {}
"""Active task object is of the format:
[{
    "user":                     globus username
    "globus_id":                globus job ID of upload
    "files":        [{          list of files included in task, each with
            "path": "file1",        ...file path, which is updated with path-on-disk once completed
            "md": {}                ...metadata to be associated with that file
        }, {...}, ...],
    "received":                 timestamp when task was sent to monitor API
    "completed":                timestamp when task was completed (including errors and cancelled tasks)
    "status":                   can be "IN PROGRESS", "DONE", "ABORTED", "ERROR"
}, {...}, {...}, ...]"""
activeTasks = {}

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
def loadActiveTasksFromDisk():
    activePath = config.service.activePath

    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(activePath):
        if os.path.exists(activePath+".backup"):
            shutil.copyfile(activePath+".backup", activePath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(activePath, 'w')
            f.write("{}")
            f.close()

    # Load contents of file into memory
    f = open(activePath)
    activeTasks = json.load(f)
    f.close()

"""Write activeTasks from memory into file"""
def writeActiveTasksToDisk():
    # Write current file to backup location before writing current file
    activePath = config.service.activePath
    shutil.move(activePath, activePath+".backup")
    f = open(activePath, 'w')
    f.write(json.dumps(activeTasks))
    f.close()

"""Write a completed task onto disk in appropriate folder hierarchy"""
def writeCompletedTaskToDisk(task):
    completedPath = config.service.completedPath
    taskID = task.globus_id

    # Create root directory if necessary
    if not os.path.exists(completedPath):
        os.mkdir(completedPath)

    # Create nested hierarchy folders if needed, to hopefully avoid a long flat list
    treeLv1 = os.path.join(completedPath, taskID[:2])
    treeLv2 = os.path.join(treeLv1, taskID[2:4])
    treeLv3 = os.path.join(treeLv2, taskID[4:6])
    treeLv4 = os.path.join(treeLv3, taskID[6:8])

    # e.g. TaskID "eaca1f1a-d400-11e5-975b-22000b9da45e" would go to:
    #   <completedPath>/ea/ca/1f/1a/eaca1f1a-d400-11e5-975b-22000b9da45e.json

    for dir in [treeLv1, treeLv2, treeLv3, treeLv4]:
        if not os.path.exists(dir):
            os.mkdir(dir)

    # Write to json file with task ID as filename
    dest = os.path.join(treeLv4, taskID+".json")
    f = open(dest, 'w')
    f.write(json.dumps(task))
    f.close()

    # TODO: Write completed jobs to individual .json files by task ID w/ folder nesting
    # REDO AS INDIVIDUAL JSON FILES
    completedPath = config.service.completedPath
    f = open(completedPath, 'w')
    f.write(json.dumps(completedTasks))
    f.close()

# ----------------------------------------------------------
# API COMPONENTS
# ----------------------------------------------------------
"""API - Post new globus tasks to be monitored"""
class GlobusMonitor(restful.Resource):

    """Return list of all active tasks"""
    def get(self):
        return activeTasks, 200

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
                    "globus_id": globus_id,
                    "files": file_list,
                    "received": datetime.datetime.now(),
                    "completed": None,
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

    """Remove task from active tasks"""
    def delete(self, globusID):
        # Set status = "ABORTED" and write to completed
        return 201

# Add a new Globus id that should be monitored
api.add_resource(GlobusMonitor, '/tasks')
# Check to see if Globus id is finished
api.add_resource(GlobusTask, '/tasks/<string:globusID>')

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access tokens for valid accounts"""
def generateAuthTokens():
    for validUser in config.globus.validUsers.keys:
        config.globus.validUsers[validUser].authToken = goauth.get_access_token(
                username=validUser,
                password=config.globus.validUsers[validUser].password
            ).token

"""Query Globus API re: globusID to get current transfer status"""
def checkGlobusStatus(taskObj):
    if globusID in activeTasks.keys:
        taskUser = activeTasks[globusID].user
        taskToken = config.globus.validUsers[taskUser].authToken

        api = TransferAPIClient(username=taskUser, goauth=taskToken)
        status_code, status_message, data = api.task_list()
        for gid in data.DATA:
            # Check if status has changed from what is already known
            status = data.DATA[gid].status
            activeTasks[gid].status = status
            return status

"""Continually check globus API for task updates"""
def globusMonitorLoop():
    refreshTimer = 0
    while True:
        time.sleep(1)
        # For tasks whose status is still in-progress, check Globus for transfer status
        for task in activeTasks:
            status = checkGlobusStatus(task)

            if status == "DONE":
                # Write file path to actual location on disk before notifying Clowder
                task.status = "DONE"
                task.completed = datetime.datetime.now()
                task.path = "" # TODO: iterate through files and update each path

                notifyClowderOfCompletedTask(task)

                writeCompletedTaskToDisk(task)
                del activeTasks[task.globus_id]
                writeActiveTasksToDisk()

            elif status == "ERROR":
                # Write task as completed with an error
                task.status = "ERROR"
                task.completed = datetime.datetime.now()
                task.path = "" # TODO: iterate through files and update each path

                writeCompletedTaskToDisk(task)
                del activeTasks[task.globus_id]
                writeActiveTasksToDisk()

        # Refresh auth tokens every 12 hours
        refreshTimer += 1
        if refreshTimer >= 43200:
            generateAuthTokens()
            refreshTimer = 0

"""Send Clowder necessary details to load local file after Globus transfer complete"""
def notifyClowderOfCompletedTask(taskObj):
    pass


if __name__ == '__main__':
    loadConfigFromFile()
    loadActiveTasksFromDisk()
    generateAuthTokens()

    # Create thread for API to begin listening
    thread.start_new_thread(app.run, kwargs={
        "host": "0.0.0.0",
        "port": int(config.api.port),
        "debug": True
    })
    print("API now listening on port "+config.api.port)
    # TODO: look @ flask auth to use basic auth (oauth)

    # Create thread for service to begin monitoring
    thread.start_new_thread(globusMonitorLoop)
    print("Service now monitoring Globus tasks")

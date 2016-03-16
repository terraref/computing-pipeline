#!/usr/bin/python

import os, json
from flask import Flask, request
from flask.ext import restful
from flask_restful import reqparse, abort, Api, Resource

from globusonline.transfer import api_client

app = Flask(__name__) 
api = restful.Api(app)

# TODO: Move these parameters somewhere else?
userListPath = "/usr/local/data/globus_users.json"
activeJobsPath = "/usr/local/data/active_jobs.json"
resultsPath = "/usr/local/data/results.json"

validUsers = []
activeJobs = {}
completed  = {}

"""Load list of accepted Globus users whose jobs can be monitored"""
def loadValidUsersFromFile():
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
    if not os.path.exists(activeJobsPath):
        # Create an empty file if it doesn't exist already
        jobFile = open(activeJobsPath, 'w')
        jobFile.write("{}")
        jobFile.close()
    jobFile = open(activeJobsPath)
    activeJobs = json.load(jobFile)
    jobFile.close()

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
    jobFile = open(activeJobsPath, 'w')
    jobFile.write(json.dumps(activeJobs))
    jobFile.close()

    doneFile = open(resultsPath, 'w')
    doneFile.write(json.dumps(completed))
    doneFile.close()

"""Query Globus API re: globusID to get current transfer status"""
def checkGlobusStatus(globusID):
    # Examples here: https://github.com/globusonline/transfer-api-client-python/tree/master/globusonline/transfer/api_client/examples
    api = api_client.TransferAPIClient(username="username",
                                       cert_file="path/to/client/credential",
                                       key_file="path/to/client/credential")
    status_code, status_message, data = api.task_list()

"""Send Clowder necessary details to load local file after Globus transfer complete"""
def notifyClowderOfCompletedFile(globusID):
    pass



class GlobusMonitor(restful.Resource):

    def post(self):
        """Add new Globus ID from a user to be monitored"""

        json_data = request.get_json(force=True)
        for job in json_data:
            globus_user = str(job['user'])
            globus_id = str(job['globus_id'])
            file_list = str(job['files'])

            # Check if globus username is known

            # Add to local queue that is checked all the time and saved to disk
            activeJobs[globus_id] = {
                "user": globus_user,
                "files": file_list,
                "status": "IN PROGRESS"
            }

            writeJobListsToFile()

        return 201

class GlobusJob(restful.Resource):

    def get(self, globusID):
        """Check if the Globus ID is finished, in progress, or an error has occurred"""

        if globusID not in activeJobs.keys or completed.keys:
            return "Globus ID not found", 404
        elif globusID in activeJobs.keys:
            # Check with Globus API
            return activeJobs[globusID], 200
        else:
            return "COMPLETE", 200



# Initialize user list and jobs from files on disk (if any) during startup
loadValidUsersFromFile()
loadJobListsFromFile()

# ENDPOINTS ----------------
# Add a new Globus id that should be monitored
api.add_resource(GlobusMonitor, '/upload')

# Check to see if Globus id is finished
api.add_resource(GlobusJob, '/upload/<string:globusID>')
# ----------------------------

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5454, debug=True)



# MOVE TO SEPARATE PIECE
while True:
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

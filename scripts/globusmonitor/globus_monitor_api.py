#!/usr/bin/python

import os, json
from flask import Flask, request
from flask.ext import restful
from flask_restful import reqparse, abort, Api, Resource

from globus_monitor_service import loadConfigFromFile


app = Flask(__name__) 
api = restful.Api(app)

config = {}
jobs = {}

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
            jobs[globus_id] = {
                "user": globus_user,
                "files": file_list,
                "status": "IN PROGRESS"
            }

        return 201

class GlobusJob(restful.Resource):

    def get(self, globusID):
        """Check if the Globus ID is finished, in progress, or an error has occurred"""

        if globusID not in jobs.keys:
            return "Globus ID not found", 404
        else:
            return jobs[globusID], 200


class JobList(restful.Resource):

    def get(self):
        # Return a list of active jobs
        return jobs, 200

    def put(self, globusID):
        # Update status of a given job from service
        jobs[globusID]

# Initialize user list and jobs from files on disk (if any) during startup
config = loadConfigFromFile()

# ENDPOINTS ----------------
# Add a new Globus id that should be monitored
api.add_resource(GlobusMonitor, '/upload')

# Check to see if Globus id is finished
api.add_resource(GlobusJob, '/upload/<string:globusID>')

# Fetch current jobs being monitored (for service)
api.add_resource(JobList, '/jobs')
# ----------------------------

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(config.api.port), debug=True)

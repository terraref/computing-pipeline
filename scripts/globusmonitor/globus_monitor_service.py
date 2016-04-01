#!/usr/bin/python

""" GLOBUS MONITOR SERVICE
    This will load parameters from the configFile defined below,
    and start up an API to listen on the specified port for new
    Globus task IDs. It will then monitor the specified Globus
    directory and query the Globus API until that task ID has
    succeeded or failed, and notify Clowder accordingly.
"""

import os, shutil, json, time, datetime, thread, copy
import requests
from requests.packages.urllib3.filepost import encode_multipart_formdata
from functools import wraps
from flask import Flask, request, Response
from flask.ext import restful
from flask_restful import reqparse, abort, Api, Resource
from globusonline.transfer.api_client import TransferAPIClient, APIError, ClientError, goauth


config = {}
configFile = "config.json"

"""activeTasks tracks which Globus IDs are being monitored, and is of the format:
{"globus_id": {
    "user":                     globus username
    "globus_id":                globus job ID of upload
    "contents": {
        "dataset": {                dataset used as key for set of files transferred
            "md": {}                metadata to be associated with this dataset
            "files": {              dict of files from this dataset included in task, each with
                "filename1": {
                    "name": "file1.txt",    ...filename
                    "path": "",             ...file path, which is updated with path-on-disk once completed
                    "md": {}                ...metadata to be associated with that file
                    "clowder_id": "UUID"    ...UUID of file in Clowder once it is uploaded
                },
                "filename2": {...},
                ...
            }
        },
        "dataset2": {...},
        ...
    },
    "received":                 timestamp when task was sent to monitor API
    "completed":                timestamp when task was completed (including errors and cancelled tasks)
    "status":                   can be "IN PROGRESS", "DONE", "ABORTED", "ERROR"
}, {...}, {...}, ...}"""
activeTasks = {}

"""datasetMap maps sensor/timestamp dataset name to clowder dataset UUID
{
    "datasetName": "UUID"
}
"""
datasetMap = {}

app = Flask(__name__)
api = restful.Api(app)

# ----------------------------------------------------------
# SHARED UTILS
# ----------------------------------------------------------
"""Load contents of .json file into a JSON object"""
def loadJsonFile(filename):
    f = open(filename)
    jsonObj = json.load(f)
    f.close()
    return jsonObj

"""Load object into memory from a log file, checking for .backup if main file does not exist"""
def loadLoggingDataFromDisk(logPath):
    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(logPath):
        if os.path.exists(logPath+".backup"):
            print("...loading data from "+logPath+".backup")
            shutil.copyfile(logPath+".backup", logPath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(logPath, 'w')
            f.write("{}")
            f.close()
    else:
        print("...loading data from "+logPath)

    return loadJsonFile(logPath)

"""Save object into a log file from memory, moving existing file to .backup if it exists"""
def writeLoggingDataToDisk(logPath, logData):
    print("...writing data to "+logPath)

    # Move existing copy to .backup if it exists
    if os.path.exists(logPath):
        shutil.move(logPath, logPath+".backup")

    f = open(logPath, 'w')
    f.write(json.dumps(logData))
    f.close()

"""Write a completed task onto disk in appropriate folder hierarchy"""
def writeCompletedTaskToDisk(task):
    completedPath = config['api']['completed_path']
    taskID = task['globus_id']

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
    print("...writing completed task to "+dest)
    f = open(dest, 'w')
    f.write(json.dumps(task))
    f.close()

"""Find dataset id if dataset exists, creating if necessary"""
def fetchDatasetByName(datasetName, requestsSession):
    if datasetName not in datasetMap:
        print("......creating dataset "+datasetName)
        ds = requestsSession.post(config['clowder']['host']+"/api/datasets/createempty",
                                  headers={"Content-Type": "application/json"},
                                  data='{"name": "%s"}' % datasetName)

        if ds.status_code == 200:
            dsid = ds.json()['id']
            datasetMap[datasetName] = dsid
            writeLoggingDataToDisk(config['clowder']['dataset_map'], datasetMap)
            return dsid
        else:
            # TODO: Handle errors more gracefully
            print("[ERROR] cannot create dataset ("+str(ds.status_code)+")")
            return ""
    else:
        # TODO: Check if this actually still exists before just assuming it's still there
        return datasetMap[datasetName]

# ----------------------------------------------------------
# API COMPONENTS
# ----------------------------------------------------------
"""Authentication components for API (http://flask.pocoo.org/snippets/8/)"""
def check_auth(username, password):
    """Called to check whether username/password is valid"""
    if username in config['globus']['valid_users']:
        return password == config['globus']['valid_users'][username]['password']
    else:
        return False

def authenticate():
    """Send 401 response that enables basic auth"""
    return Response("Could not authenticate. Please provide valid Globus credentials.",
                    401, {"WWW-Authenticate": 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

""" /tasks
POST new globus tasks to be monitored, or GET full list of tasks being monitored"""
class GlobusMonitor(restful.Resource):

    """Return list of all active tasks initiated by the requesting user"""
    @requires_auth
    def get(self):
        # TODO: Should this be filtered by user somehow?
        return activeTasks, 200

    """Add new Globus task ID from a known user for monitoring"""
    @requires_auth
    def post(self):
        task = request.get_json(force=True)
        taskUser = task['user']

        # Add to active list if globus username is known, and write log to disk
        if taskUser in config['globus']['valid_users']:
            print("[TASK] now monitoring task from "+taskUser+": "+task['globus_id'])

            activeTasks[task['globus_id']] = {
                "user": taskUser,
                "globus_id": task['globus_id'],
                "contents": task['contents'],
                "received": str(datetime.datetime.now()),
                "completed": None,
                "status": "IN PROGRESS"
            }
            writeLoggingDataToDisk(config['api']['active_path'], activeTasks)

        return 201

""" /tasks/<globusID>
GET details of a particular globus task, or DELETE a globus task from monitoring"""
class GlobusTask(restful.Resource):

    """Check if the Globus task ID is finished, in progress, or an error has occurred"""
    @requires_auth
    def get(self, globusID):
        # TODO: Should this require same Globus credentials as 'owner' of task?
        if globusID not in activeTasks:
            # If not in active list, check for record of completed task
            logPath = os.path.join(config['api']['completed_path'], globusID[:2], globusID[2:4], globusID[4:6], globusID[6:8], globusID+".json")
            if os.path.exists(logPath):
                return loadJsonFile(logPath), 200
            else:
                return "Globus ID not found in active or completed tasks", 404
        else:
            return activeTasks[globusID], 200

    """Remove task from active tasks being monitored"""
    @requires_auth
    def delete(self, globusID):
        if globusID in activeTasks:
            # TODO: Should this perform deletion within Globus as well? For now, just deletes from monitoring
            task = activeTasks[globusID]

            # Write task as completed with an aborted status
            task.status = "DELETED"
            task.completed = datetime.datetime.now()
            task.path = ""

            writeCompletedTaskToDisk(task)
            del activeTasks[task['globus_id']]
            writeLoggingDataToDisk(config['api']['active_path'], activeTasks)
            return 204

""" /metadata
POST metadata for a Clowder dataset without requiring Globus task pipeline"""
class MetadataLoader(restful.Resource):

    @requires_auth
    def post(self):
        req = request.get_json(force=True)
        globUser = req['user']
        clowderUser = config['clowder']['user_map'][globUser]['clowder_user']
        clowderPass = config['clowder']['user_map'][globUser]['clowder_pass']

        sess = requests.Session()
        sess.auth = (clowderUser, clowderPass)

        dsid = fetchDatasetByName(req['dataset'], sess)
        print("......adding metadata to dataset "+dsid)
        dsmd = sess.post(config['clowder']['host']+"/api/datasets/"+dsid+"/metadata",
                         headers={'Content-Type':'application/json'},
                         data=json.dumps(req['md']))

        if dsmd.status_code != 200:
            print("[ERROR] cannot add dataset metadata ("+str(dsmd.status_code)+")")

        return dsmd.status_code

api.add_resource(GlobusMonitor, '/tasks')
api.add_resource(GlobusTask, '/tasks/<string:globusID>')
api.add_resource(MetadataLoader, '/metadata')

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access tokens for valid accounts"""
def generateAuthTokens():
    for validUser in config['globus']['valid_users']:
        print("...generating auth token for "+validUser)
        config['globus']['valid_users'][validUser]['auth_token'] = goauth.get_access_token(
                username=validUser,
                password=config['globus']['valid_users'][validUser]['password']
            ).token

"""Query Globus API to get current transfer status of a given task"""
def getGlobusStatus(task):
    authToken = config['globus']['valid_users'][task['user']]['auth_token']
    api = TransferAPIClient(username=task['user'], goauth=authToken)
    try:
        status_code, status_message, task_data = api.task(task['globus_id'])
    except (APIError, ClientError) as e:
        # Refreshing auth tokens and retry
        generateAuthTokens()
        authToken = config['globus']['valid_users'][task['user']]['auth_token']
        api = TransferAPIClient(username=task['user'], goauth=authToken)
        status_code, status_message, task_data = api.task(task['globus_id'])

    if status_code == 200:
        return task_data['status']
    else:
        return "UNKNOWN ("+status_code+": "+status_message+")"

"""Send Clowder necessary details to load local file after Globus transfer complete"""
def notifyClowderOfCompletedTask(task):
    # Verify that globus user has a mapping to clowder credentials in config file
    globUser = task['user']
    userMap = config['clowder']['user_map']

    if globUser in userMap:
        print("...notifying Clowder of task completion: "+task['globus_id'])
        clowderHost = config['clowder']['host']
        clowderUser = userMap[globUser]['clowder_user']
        clowderPass = userMap[globUser]['clowder_pass']

        sess = requests.Session()
        sess.auth = (clowderUser, clowderPass)

        for ds in task['contents']:
            dsid = fetchDatasetByName(ds, sess)

            # Assign dataset-level metadata if provided
            if "md" in task['contents'][ds]:
                print("......adding metadata to dataset "+ds)
                dsmd = sess.post(config['clowder']['host']+"/api/datasets/"+dsid+"/metadata",
                          headers={'Content-Type':'application/json'},
                          data=json.dumps(task['contents'][ds]['md']))
                if dsmd.status_code != 200:
                    print("[ERROR] cannot add dataset metadata ("+str(dsmd.status_code)+")")

            # Add local files to dataset by path
            for f in task['contents'][ds]['files']:
                fobj = task['contents'][ds]['files'][f]
                print("......adding file '"+fobj['name']+"' to "+ds)
                print(fobj['path'])
                # Boundary encoding from http://stackoverflow.com/questions/17982741/python-using-reuests-library-for-multipart-form-data
                (content, header) = encode_multipart_formdata([
                    ("file",'{"path":"%s"%s}' % (
                        fobj['path'],
                        ', "md":'+json.dumps(fobj['md']) if 'md' in fobj else ""
                    ))])

                fi = sess.post(clowderHost+"/api/uploadToDataset/"+dsid, data=content, headers={'Content-Type':header})
                if fi.status_code != 200:
                    print("[ERROR] cannot upload file ("+str(fi.status_code)+")")
                else:
                    activeTasks[task['globus_id']]['contents'][ds]['files'][f]['clowder_id'] = json.loads(fi.text)['id']

    else:
        print("[ERROR] cannot find clowder user credentials for Globus user "+globUser)

"""Continually check Globus API for task updates"""
def globusMonitorLoop():
    authWait = 0
    globWait = 0
    while True:
        time.sleep(1)
        authWait += 1
        globWait += 1

        # Check with Globus for any status updates on monitored tasks
        if globWait >= config['globus']['transfer_update_frequency_secs']:
            # Use copy of task list so it doesn't change during iteration
            currentActiveTasks = copy.copy(activeTasks)
            for globusID in currentActiveTasks:
                task = activeTasks[globusID]
                globusStatus = getGlobusStatus(task)

                if globusStatus in ["SUCCEEDED", "FAILED"]:
                    print("[TASK] status update for "+globusID+": "+globusStatus)

                    # Update task parameters
                    task['status'] = globusStatus
                    task['completed'] = str(datetime.datetime.now())
                    for ds in task['contents']:
                        if 'files' in task['contents'][ds]:
                            for f in task['contents'][ds]['files']:
                                fobj = task['contents'][ds]['files'][f]
                                fobj['path'] = os.path.join(config['globus']['incoming_files_path'], fobj['path'], fobj["name"])

                    # Notify Clowder to process file if transfer successful
                    if globusStatus == "SUCCEEDED":
                        notifyClowderOfCompletedTask(task)

                    # Write out results file, then delete from active list and write log file
                    writeCompletedTaskToDisk(task)
                    del activeTasks[globusID]
                    writeLoggingDataToDisk(config['api']['active_path'], activeTasks)
            globWait = 0

        # Refresh auth tokens periodically
        if authWait >= config['globus']['authentication_refresh_frequency_secs']:
            generateAuthTokens()
            authWait = 0

if __name__ == '__main__':
    print("...loading configuration from "+configFile)
    config = loadJsonFile(configFile)
    datasetMap = loadLoggingDataFromDisk(config['clowder']['dataset_map'])
    activeTasks = loadLoggingDataFromDisk(config['api']['active_path'])
    generateAuthTokens()

    # Create thread for service to begin monitoring
    thread.start_new_thread(globusMonitorLoop, ())
    print("*** Service now monitoring Globus tasks ***")

    # Create thread for API to begin listening - requires valid Globus user/pass
    print("API now listening on port "+config['api']['port'])
    app.run(host="0.0.0.0", port=int(config['api']['port']), debug=False)

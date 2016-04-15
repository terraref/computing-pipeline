#!/usr/bin/python

""" GLOBUS MONITOR SERVICE
    This will load parameters from the configFile defined below,
    and start up an API to listen on the specified port for new
    Globus task IDs. It will then monitor the specified Globus
    directory and query the Globus API until that task ID has
    succeeded or failed, and notify Clowder accordingly.
"""

import os, shutil, json, time, datetime, thread, copy, atexit, collections, fcntl
import requests
from io import BlockingIOError
from urllib3.filepost import encode_multipart_formdata
from functools import wraps
from flask import Flask, request, Response
from flask.ext import restful
from flask_restful import reqparse, abort, Api, Resource
from globusonline.transfer.api_client import TransferAPIClient, APIError, ClientError, goauth

rootPath = "/home/globusmonitor/"

"""
Config file has 2 important entries which do not have default values:
{
    "globus": {
*** (1) The valid_users sub-object describes which users can submit jobs ***
        "valid_users": {
            <globus username>: {
                "password": <globus password>,
                "endpoint_id": <globus endpoint ID corresponding to user>
            }
        }
    },
    "clowder": {
        "user_map": {
*** (2) The user_map sub-object maps a Globus user to the Clowder credentials that upload that user's files. ***
            <globus username>: {
                "clowder_user": <clowder username>
                "clowder_pass": <clowder password>
            }
        }
    }
"""
config = {}
logFile = None

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

"""Maps dataset/collection name to clowder UUID
{
    "name": "UUID"
}
"""
datasetMap = {}
collectionMap = {}

app = Flask(__name__)
api = restful.Api(app)

# ----------------------------------------------------------
# SHARED UTILS
# ----------------------------------------------------------
def openLog():
    global logFile

    logPath = config["log_path"]

    # If there's a current log file, store it as log1.txt, log2.txt, etc.
    if os.path.exists(logPath):
        i = 1
        backupLog = logPath.replace(".txt", "_"+str(i)+".txt")
        while os.path.exists(backupLog):
            i+=1
            backupLog = logPath.replace(".txt", "_"+str(i)+".txt")
        shutil.move(logPath, backupLog)

    logFile = open(logPath, 'w+')

def closeLog():
    logFile.close()

"""If metadata keys have periods in them, Clowder will reject the metadata"""
def clean_json_keys(jsonobj):
    clean_json = {}
    for key in jsonobj.keys():
        try:
            jsonobj[key].keys() # Is this a json object?
            clean_json[key.replace(".","_")] = clean_json_keys(jsonobj[key])
        except:
            clean_json[key.replace(".","_")] = jsonobj[key]

    return clean_json

"""Attempt to lock a file so API and monitor don't write at once, and wait if unable"""
def lockFile(f):
    # From http://tilde.town/~cristo/file-locking-in-python.html
    while True:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError as e:
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
    logFile.write("["+type+"] "+message+"\n")

"""Return small JSON object with information about monitor health"""
def getStatus():
    activeTaskCount = len(activeTasks)
    datasetsCreated = len(datasetMap)

    completedTasks = 0
    for root, dirs, filelist in os.walk(config['completed_tasks_path']):
        completedTasks += len(filelist)

    return {
        "active_task_count": activeTaskCount,
        "datasets_created": datasetsCreated,
        "completed_globus_tasks": completedTasks
    }

"""Load contents of .json file into a JSON object"""
def loadJsonFile(filename):
    f = open(filename)
    jsonObj = json.load(f)
    f.close()
    return jsonObj

"""Load object into memory from a log file, checking for .backup if main file does not exist"""
def loadDataFromDisk(logPath):
    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(logPath):
        if os.path.exists(logPath+".backup"):
            log("...loading data from "+logPath+".backup")
            shutil.copyfile(logPath+".backup", logPath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(logPath, 'w')
            f.write("{}")
            f.close()
    else:
        log("...loading data from "+logPath)

    return loadJsonFile(logPath)

"""Save object into a log file from memory, moving existing file to .backup if it exists"""
def writeDataToDisk(logPath, logData):
    log("...writing data to "+logPath)

    # Move existing copy to .backup if it exists
    if os.path.exists(logPath):
        shutil.move(logPath, logPath+".backup")

    f = open(logPath, 'w')
    lockFile(f)
    f.write(json.dumps(logData))
    f.close()

"""Write a completed task onto disk in appropriate folder hierarchy"""
def writeCompletedTaskToDisk(task):
    completedPath = config['completed_tasks_path']
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
    log("...writing completed task to "+dest)
    f = open(dest, 'w')
    f.write(json.dumps(task))
    f.close()

"""Find dataset id if dataset exists, creating if necessary"""
def fetchDatasetByName(datasetName, requestsSession):
    if datasetName not in datasetMap:
        ds = requestsSession.post(config['clowder']['host']+"/api/datasets/createempty",
                                  headers={"Content-Type": "application/json"},
                                  data='{"name": "%s"}' % datasetName)

        if ds.status_code == 200:
            dsid = ds.json()['id']
            datasetMap[datasetName] = dsid
            log("created dataset "+datasetName+" ("+dsid+")")
            writeDataToDisk(config['dataset_map_path'], datasetMap)
            addDatasetToSpacesCollections(datasetName, dsid, requestsSession)
            return dsid
        else:
            log("cannot create dataset ("+str(ds.status_code)+")", "ERROR")
            return ""

    else:
        # We have a record of it, but check that it still exists before returning the ID
        dsid = datasetMap[datasetName]
        ds = requestsSession.get(config['clowder']['host']+"/api/datasets/"+dsid)
        if ds.status_code == 200:
            log("dataset "+datasetName+" already exists ("+dsid+")")
            return dsid
        else:
            # Query the database just in case, before giving up and creating a new dataset
            dstitlequery = requestsSession.get(config['clowder']['host']+"/api/datasets?title="+datasetName)
            if dstitlequery.status_code == 200:
                results = dstitlequery.json()
                if len(results) > 0:
                    return results[0]['id']
                else:
                    log("cannot find dataset "+dsid+"; creating new dataset "+datasetName)
                    # Could not find dataset so we'll just delete the record and create a new one
                    del datasetMap[datasetName]
                    return fetchDatasetByName(datasetName, requestsSession)
            else:
                log("cannot find dataset "+dsid+"; creating new dataset "+datasetName)
                # Could not find dataset so we'll just delete the record and create a new one
                del datasetMap[datasetName]
                return fetchDatasetByName(datasetName, requestsSession)

"""Find dataset id if dataset exists, creating if necessary"""
def fetchCollectionByName(collectionName, requestsSession):
    if collectionName not in collectionMap:
        coll = requestsSession.post(config['clowder']['host']+"/api/collections",
                                  headers={"Content-Type": "application/json"},
                                  data='{"name": "%s", "description": ""}' % collectionName)

        if coll.status_code == 200:
            collid = coll.json()['id']
            collectionMap[collectionName] = collid
            log("created collection "+collectionName+" ("+collid+")")
            writeDataToDisk(config['collection_map_path'], collectionMap)
            # Add new collection to primary space if defined
            if config['clowder']['primary_space'] != "":
                requestsSession.post(config['clowder']['host']+"/api/spaces/%s/addCollectionToSpace/%s" %
                                     (config['clowder']['primary_space'], collid))
            return collid
        else:
            log("cannot create collection ("+str(coll.status_code)+")", "ERROR")
            return ""

    else:
        # We have a record of it, but check that it still exists before returning the ID
        collid = collectionMap[collectionName]
        coll = requestsSession.get(config['clowder']['host']+"/api/collections/"+collid)
        if coll.status_code == 200:
            log("collection "+collectionName+" already exists ("+collid+")")
            return collid
        else:
            # Query the database just in case, before giving up and creating a new dataset
            colltitlequery = requestsSession.get(config['clowder']['host']+"/api/collections?title="+collectionName)
            if colltitlequery.status_code == 200:
                results = colltitlequery.json()
                if len(results) > 0:
                    return results[0]['id']
                else:
                    log("cannot find collection "+collid+"; creating new collection "+collectionName)
                    # Could not find collection so we'll just delete the record and create a new one
                    del collectionMap[collectionName]
                    return fetchCollectionByName(collectionName, requestsSession)
            else:
                log("cannot find collection "+collid+"; creating new collection "+collectionName)
                # Could not find collection so we'll just delete the record and create a new one
                del collectionMap[collectionName]
                return fetchCollectionByName(collectionName, requestsSession)

"""Add dataset to Space and Sensor, Date Collections"""
def addDatasetToSpacesCollections(datasetName, datasetID, requestsSession):
    sensorName = datasetName.split(" - ")[0]
    timestamp = datasetName.split(" - ")[1].split("__")[0]

    sensColl = fetchCollectionByName(sensorName, requestsSession)
    if sensColl != "":
        sc = requestsSession.post(config['clowder']['host']+"/api/collections/%s/datasets/%s" % (sensColl, datasetID))
        if sc.status_code != 200:
            log("cannot add ds "+datasetID+" to coll "+sensColl+" ("+str(sc.status_code)+")")

    timeColl = fetchCollectionByName(timestamp, requestsSession)
    if timeColl != "":
        tc = requestsSession.post(config['clowder']['host']+"/api/collections/%s/datasets/%s" % (timeColl, datasetID))
        if tc.status_code != 200:
            log("cannot add ds "+datasetID+" to coll "+timeColl+" ("+str(tc.status_code)+")")

    if config['clowder']['primary_space'] != "":
        spid = config['clowder']['primary_space']
        sp = requestsSession.post(config['clowder']['host']+"/api/spaces/%s/addDatasetToSpace/%s" % (spid, datasetID))
        if sp.status_code != 200:
            log("cannot add ds "+datasetID+" to space "+spid+" ("+str(sp.status_code)+")")

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
            log("now monitoring task from "+taskUser+": "+task['globus_id'])

            activeTasks[task['globus_id']] = {
                "user": taskUser,
                "globus_id": task['globus_id'],
                "contents": task['contents'],
                "received": str(datetime.datetime.now()),
                "completed": None,
                "status": "IN PROGRESS"
            }
            writeDataToDisk(config['active_tasks_path'], activeTasks)

            return 201
        else:
            return "Task user not in list of authorized submitters", 401

""" /tasks/<globusID>
GET details of a particular globus task, or DELETE a globus task from monitoring"""
class GlobusTask(restful.Resource):

    """Check if the Globus task ID is finished, in progress, or an error has occurred"""
    @requires_auth
    def get(self, globusID):
        # TODO: Should this require same Globus credentials as 'owner' of task?
        if globusID not in activeTasks:
            # If not in active list, check for record of completed task
            logPath = os.path.join(config['completed_tasks_path'], globusID[:2], globusID[2:4], globusID[4:6], globusID[6:8], globusID+".json")
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
            writeDataToDisk(config['active_tasks_path'], activeTasks)
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

        md = clean_json_keys(req['md'])
        dsid = fetchDatasetByName(req['dataset'], sess)
        log("adding metadata to dataset "+dsid)
        dsmd = sess.post(config['clowder']['host']+"/api/datasets/"+dsid+"/metadata",
                         headers={'Content-Type':'application/json'},
                         data=json.dumps(md))

        if dsmd.status_code != 200:
            log("cannot add dataset metadata ("+str(dsmd.status_code)+")", "ERROR")

        return dsmd.status_code

""" / status
Return basic information about monitor for health checking"""
class MonitorStatus(restful.Resource):

    def get(self):
        return getStatus(), 200

api.add_resource(GlobusMonitor, '/tasks')
api.add_resource(GlobusTask, '/tasks/<string:globusID>')
api.add_resource(MetadataLoader, '/metadata')
api.add_resource(MonitorStatus, '/status')

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access tokens for valid accounts"""
def generateAuthTokens():
    for validUser in config['globus']['valid_users']:
        log("...generating auth token for "+validUser)
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
        try:
            # Refreshing auth tokens and retry
            generateAuthTokens()
            authToken = config['globus']['valid_users'][task['user']]['auth_token']
            api = TransferAPIClient(username=task['user'], goauth=authToken)
            status_code, status_message, task_data = api.task(task['globus_id'])
        except (APIError, ClientError) as e:
            log("problem checking Globus transfer status", "ERROR")
            status_code = 503

    if status_code == 200:
        return task_data['status']
    else:
        return None

"""Send Clowder necessary details to load local file after Globus transfer complete"""
def notifyClowderOfCompletedTask(task):
    # Verify that globus user has a mapping to clowder credentials in config file
    globUser = task['user']
    userMap = config['clowder']['user_map']

    if globUser in userMap:
        log("notifying Clowder of task completion: "+task['globus_id'])
        clowderHost = config['clowder']['host']
        clowderUser = userMap[globUser]['clowder_user']
        clowderPass = userMap[globUser]['clowder_pass']

        sess = requests.Session()
        sess.auth = (clowderUser, clowderPass)

        for ds in task['contents']:
            dsid = fetchDatasetByName(ds, sess)

            # Assign dataset-level metadata if provided
            if "md" in task['contents'][ds]:
                log("adding metadata to dataset "+ds)

                md = clean_json_keys(task['contents'][ds]['md'])

                dsmd = sess.post(config['clowder']['host']+"/api/datasets/"+dsid+"/metadata",
                          headers={'Content-Type':'application/json'},
                          data=json.dumps(md))
                if dsmd.status_code != 200:
                    log("cannot add dataset metadata ("+str(dsmd.status_code)+" - "+dsmd.text+")", "ERROR")
                else:
                    # Remove metadata from activeTasks on success even if file upload fails in next step, so we don't repeat md
                    del activeTasks[task['globus_id']][ds]['md']

            # Add local files to dataset by path
            if 'files' in task['contents'][ds]:
                for f in task['contents'][ds]['files']:
                    fobj = task['contents'][ds]['files'][f]
                    log("adding file '"+fobj['path']+"' to "+ds)
                    # Boundary encoding from http://stackoverflow.com/questions/17982741/python-using-reuests-library-for-multipart-form-data
                    (content, header) = encode_multipart_formdata([
                        ("file",'{"path":"%s"%s}' % (
                            fobj['path'],
                            ', "md":'+json.dumps(fobj['md']) if 'md' in fobj else ""
                        ))])

                    fi = sess.post(clowderHost+"/api/uploadToDataset/"+dsid, data=content, headers={'Content-Type':header})
                    if fi.status_code != 200:
                        log("cannot upload file "+fobj['path']+" ("+str(fi.status_code)+")", "ERROR")
                        return False
                    else:
                        activeTasks[task['globus_id']]['contents'][ds]['files'][f]['clowder_id'] = json.loads(fi.text)['id']
                        return True
    else:
        log("cannot find clowder user credentials for Globus user "+globUser, "ERROR")
        return False

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
            currentActiveTasks = copy.deepcopy(activeTasks)
            for globusID in currentActiveTasks:
                task = activeTasks[globusID]
                globusStatus = getGlobusStatus(task)

                # If this isn't done yet, leave the task active so we can try again next time
                if globusStatus in ["SUCCEEDED", "FAILED"]:
                    log("status update for "+globusID+": "+globusStatus)

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
                        clowderDone = notifyClowderOfCompletedTask(task)
                        if clowderDone:
                            # Write out results file, then delete from active list and write log file
                            writeCompletedTaskToDisk(task)
                            del activeTasks[globusID]
                            writeDataToDisk(config['active_tasks_path'], activeTasks)

                    # Write failed transfers out to completed folder without Clowder
                    elif globusStatus == "FAILED":
                        writeCompletedTaskToDisk(task)
                        del activeTasks[globusID]
                        writeDataToDisk(config['active_tasks_path'], activeTasks)

            globWait = 0
            writeDataToDisk(config["status_log_path"], getStatus())

        # Refresh auth tokens periodically
        if authWait >= config['globus']['authentication_refresh_frequency_secs']:
            generateAuthTokens()
            authWait = 0

if __name__ == '__main__':
    # Try to load custom config file, falling back to default values where not overridden
    config = loadJsonFile(os.path.join(rootPath, "config_default.json"))
    if os.path.exists(os.path.join(rootPath, "data/config_custom.json")):
        print("...loading configuration from config_custom.json")
        config = updateNestedDict(config, loadJsonFile(os.path.join(rootPath, "data/config_custom.json")))
    else:
        print("...no custom configuration file found. using default values")
    openLog()
    atexit.register(closeLog)

    datasetMap = loadDataFromDisk(config['dataset_map_path'])
    collectionMap = loadDataFromDisk(config['collection_map_path'])
    activeTasks = loadDataFromDisk(config['active_tasks_path'])
    generateAuthTokens()

    # Create thread for service to begin monitoring
    thread.start_new_thread(globusMonitorLoop, ())
    log("*** Service now monitoring Globus tasks ***")

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    log("*** API now listening on "+config['api']['ip_address']+":"+apiPort+" ***")
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

#!/usr/bin/python

""" GLOBUS MONITOR SERVICE
    This will load parameters from the configFile defined below,
    and start up an API to listen on the specified port for new
    Globus task IDs. It will then monitor the specified Globus
    directory and query the Globus API until that task ID has
    succeeded or failed, and notify Clowder accordingly.
"""

import os, shutil, json, time, datetime, thread, copy, atexit, collections, fcntl
import logging, logging.config, logstash
import requests
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from io import BlockingIOError
from urllib3.filepost import encode_multipart_formdata
from functools import wraps
from flask import Flask, request, Response
from flask.ext import restful
from flask_restful import reqparse, abort, Api, Resource
from globusonline.transfer.api_client import TransferAPIClient, APIError, ClientError, goauth

rootPath = "/home/globusmonitor/computing-pipeline/scripts/globusmonitor"

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

"""List of Globus IDs that need to be moved into Clowder; details stored in /completed folder"""
unprocessedTasks = []

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
"""Create copy of dict in safe manner for multi-thread access (won't change during copy iteration)"""
def safeCopy(obj):
    # Iterate across a copy since we'll be changing object
    copied = False
    while not copied:
        try:
            newObj = copy.deepcopy(obj)
            copied = True
        except RuntimeError:
            # This can occur on the deepcopy step if another thread is accessing object
            time.sleep(0.1)

    return newObj

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
        except (BlockingIOError, IOError) as e:
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

"""Return small JSON object with information about monitor health"""
def getStatus():
    activeTaskCount = len(activeTasks)

    return {
        "active_task_count": activeTaskCount,
        "unprocessed_task_count": len(unprocessedTasks),
        "next_unprocessed_task": unprocessedTasks[0] if len(unprocessedTasks) > 0 else ""
    }

"""Load contents of .json file into a JSON object"""
def loadJsonFile(filename):
    try:
        f = open(filename)
        jsonObj = json.load(f)
        f.close()
        return jsonObj
    except IOError:
        logger.error("- unable to open %s" % filename)
        return {}

"""Find dataset id if dataset exists, creating if necessary"""
def fetchDatasetByName(datasetName, requestsSession, spaceOverrideId=None):
    if datasetName not in datasetMap:
        # Fetch collection & space IDs (creating collections if necessary) to post with the new dataset
        if datasetName.find(" - ") > -1:
            sensorName = datasetName.split(" - ")[0]
            timestamp = datasetName.split(" - ")[1].split("__")[0]
            sensCollId = fetchCollectionByName(sensorName, requestsSession)
            timeCollId = fetchCollectionByName(timestamp, requestsSession)
        if not spaceOverrideId and config['clowder']['primary_space'] != "":
            spaceOverrideId = config['clowder']['primary_space']
        spaceId = ', "space": ["%s"]' % spaceOverrideId if spaceOverrideId else ''

        dataObj = '{"name": "%s", "collection": ["%s","%s"]%s}' % (datasetName, sensCollId, timeCollId, spaceId)
        ds = requestsSession.post(config['clowder']['host']+"/api/datasets/createempty",
                                  headers={"Content-Type": "application/json"},
                                  data=dataObj)

        if ds.status_code == 200:
            dsid = ds.json()['id']
            datasetMap[datasetName] = dsid
            writeDatasetRecordToDatabase(datasetName, dsid)
            logger.info("++ created dataset %s (%s)" % (datasetName, dsid), extra={
                "dataset_id": dsid,
                "dataset_name": datasetName,
                "action": "DATASET CREATED"
            })
            return dsid
        else:
            logger.error("- cannot create dataset (%s: %s)" % (ds.status_code, ds.text))
            return None

    else:
        # We have a record of it, but check that it still exists before returning the ID
        dsid = datasetMap[datasetName]
        return dsid

        # TODO: re-enable this check once backlog is caught up
        ds = requestsSession.get(config['clowder']['host']+"/api/datasets/"+dsid)
        if ds.status_code == 200:
            logger.info("- dataset %s already exists (%s)" % (datasetName, dsid))
            return dsid
        else:
            # Query the database just in case, before giving up and creating a new dataset
            dstitlequery = requestsSession.get(config['clowder']['host']+"/api/datasets?title="+datasetName)
            if dstitlequery.status_code == 200:
                results = dstitlequery.json()
                if len(results) > 0:
                    return results[0]['id']
                else:
                    logger.error("- cannot find dataset %s; creating new dataset %s" % (dsid, datasetName))
                    # Could not find dataset so we'll just delete the record and create a new one
                    del datasetMap[datasetName]
                    return fetchDatasetByName(datasetName, requestsSession, spaceOverride)
            else:
                logger.error("- cannot find dataset %s; creating new dataset %s" % (dsid, datasetName))
                # Could not find dataset so we'll just delete the record and create a new one
                del datasetMap[datasetName]
                return fetchDatasetByName(datasetName, requestsSession, spaceOverride)

"""Find list of file objects in a given dataset"""
def fetchDatasetFileList(datasetId, requestsSession):
    clowkey = config['clowder']['secret_key']
    filelist = requests.get(config['clowder']['host']+"/api/datasets/%s/listFiles?key=%s" % (datasetId, clowkey),
                              headers={"Content-Type": "application/json"})

    if filelist.status_code == 200:
        return filelist.json()
    else:
        logger.error("- cannot find file list for dataset %s" % datasetId)
        return []

"""Find dataset id if dataset exists, creating if necessary"""
def fetchCollectionByName(collectionName, requestsSession):
    if collectionName not in collectionMap:
        coll = requestsSession.post(config['clowder']['host']+"/api/collections",
                                    headers={"Content-Type": "application/json"},
                                    data='{"name": "%s", "description": ""}' % collectionName)
        time.sleep(1)

        if coll.status_code == 200:
            collid = coll.json()['id']
            collectionMap[collectionName] = collid
            writeCollectionRecordToDatabase(collectionName, collid)
            logger.info("++ created collection %s (%s)" % (collectionName, collid), extra={
                "collection_id": collid,
                "collection_name": collectionName,
                "action": "CREATED COLLECTION"
            })
            # Add new collection to primary space if defined
            if config['clowder']['primary_space'] != "":
                requestsSession.post(config['clowder']['host']+"/api/spaces/%s/addCollectionToSpace/%s" %
                                     (config['clowder']['primary_space'], collid))
            return collid
        else:
            logger.error("- cannot create collection (%s: %s)" % (coll.status_code, coll.text))
            return None

    else:
        # We have a record of it, but check that it still exists before returning the ID
        collid = collectionMap[collectionName]
        return collid

        # TODO: re-enable this check once backlog is caught up
        coll = requestsSession.get(config['clowder']['host']+"/api/collections/"+collid)
        if coll.status_code == 200:
            logger.info("- collection %s already exists (%s)" % (collectionName, collid))
            return collid
        else:
            # Query the database just in case, before giving up and creating a new dataset
            colltitlequery = requestsSession.get(config['clowder']['host']+"/api/collections?title="+collectionName)
            if colltitlequery.status_code == 200:
                results = colltitlequery.json()
                if len(results) > 0:
                    return results[0]['id']
                else:
                    logger.error("- cannot find collection %s; creating new collection %s" % (collid, collectionName))
                    # Could not find collection so we'll just delete the record and create a new one
                    del collectionMap[collectionName]
                    return fetchCollectionByName(collectionName, requestsSession)
            else:
                logger.error("- cannot find collection %s; creating new collection %s" % (collid, collectionName))
                # Could not find collection so we'll just delete the record and create a new one
                del collectionMap[collectionName]
                return fetchCollectionByName(collectionName, requestsSession)


# ----------------------------------------------------------
# POSTGRES LOGGING COMPONENTS
# ----------------------------------------------------------
"""Return a connection to the PostgreSQL database"""
def connectToPostgres():
    """
    If globusmonitor database does not exist yet:
        $ initdb /home/globusmonitor/postgres/data
        $ pg_ctl -D /home/globusmonitor/postgres/data -l /home/globusmonitor/postgres/log
        $   createdb globusmonitor
    """
    try:
        conn = psycopg2.connect(dbname='globusmonitor')
    except:
        # Attempt to create database if not found
        conn = psycopg2.connect(dbname='postgres')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        curs.execute('CREATE DATABASE globusmonitor;')
        curs.close()
        conn.commit()
        conn.close()

        conn = psycopg2.connect(dbname='globusmonitor')
        initializeDatabase(conn)

    return conn

"""Create PostgreSQL database tables"""
def initializeDatabase(db_connection):
    # Table creation queries
    ct_tasks = "CREATE TABLE globus_tasks (globus_id TEXT PRIMARY KEY NOT NULL, status TEXT NOT NULL, received TEXT NOT NULL, completed TEXT, globus_user TEXT, contents JSON);"
    ct_dsets = "CREATE TABLE datasets (name TEXT PRIMARY KEY NOT NULL, clowder_id TEXT NOT NULL);"
    ct_colls = "CREATE TABLE collections (name TEXT PRIMARY KEY NOT NULL, clowder_id TEXT NOT NULL);"

    # Index creation queries
    ix_tasks = "CREATE UNIQUE INDEX globus_idx ON globus_tasks (globus_id);"
    ix_dsets = "CREATE UNIQUE INDEX dset_idx ON datasets (name);"
    ix_colls = "CREATE UNIQUE INDEX coll_idx ON collections (name);"

    # Execute each query
    curs = db_connection.cursor()
    logger.info("Creating PostgreSQL tables...")
    curs.execute(ct_tasks)
    curs.execute(ct_dsets)
    curs.execute(ct_colls)
    logger.info("Creating PostgreSQL indexes...")
    curs.execute(ix_tasks)
    curs.execute(ix_dsets)
    curs.execute(ix_colls)
    curs.close()
    db_connection.commit()

    logger.info("PostgreSQL initialization complete.")

"""Fetch a Globus task from PostgreSQL"""
def readTaskFromDatabase(globus_id):
   q_fetch = "SELECT * FROM globus_tasks WHERE globus_id = '%s'" % globus_id

   curs = psql_conn.cursor()
   logger.debug("Fetching task %s from PostgreSQL..." % globus_id)
   curs.execute(q_fetch)
   result = curs.fetchone()
   curs.close()

   if result:
       return {
           "globus_id": result[0],
           "status": result[1],
           "received": result[2],
           "completed": result[3],
           "user": result[4],
           "contents": result[5]
       }
   else:
       logger.debug("Task %s not found in PostgreSQL" % globus_id)
       return None

"""Write a Globus task into PostgreSQL, insert/update as needed"""
def writeTaskToDatabase(task):
    gid = task['globus_id']
    stat = task['status']
    recv = task['received']
    comp = task['completed']
    guser = task['user']
    jbody = json.dumps(task['contents'])

    # Attempt to insert, update if globus ID already exists
    q_insert = "INSERT INTO globus_tasks (globus_id, status, received, completed, globus_user, contents) " \
               "VALUES ('%s', '%s', '%s', '%s', '%s', '%s') " \
               "ON CONFLICT (globus_id) DO UPDATE " \
               "SET status='%s', received='%s', completed='%s', globus_user='%s', contents='%s';" % (
        gid, stat, recv, comp, guser, jbody, stat, recv, comp, guser, jbody)

    curs = psql_conn.cursor()
    logger.debug("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Fetch all Globus tasks with a particular status"""
def readTasksByStatus(status):
    """
    IN PROGRESS (received notification from sender but not yet verified complete)
         FAILED (Globus could not complete; no longer attempting to complete)
        DELETED (manually via api below)
      SUCCEEDED (verified complete; not yet uploaded into Clowder)
      PROCESSED (complete & uploaded into Clowder)
    """
    q_fetch = "SELECT * FROM globus_tasks WHERE status = '%s'" % status
    results = []

    curs = psql_conn.cursor()
    logger.debug("Fetching all %s tasks from PostgreSQL..." % status)
    curs.execute(q_fetch)
    for result in curs:
        results.append({
            "globus_id": result[0],
            "status": result[1],
            "received": result[2],
            "completed": result[3],
            "user": result[4],
            "contents": result[5]
        })
    curs.close()

    return results

"""Fetch mappings of dataset/collection name to Clowder ID"""
def readRecordsFromDatabase():
    q_fetch_datas = "SELECT * FROM datasets;"
    q_detch_colls = "SELECT * FROM collections;"

    curs = psql_conn.cursor()
    logger.debug("Fetching dataset mappings from PostgreSQL...")
    curs.execute(q_fetch_datas)
    for currds in curs:
        datasetMap[currds[0]] = currds[1]
    curs.close()

    logger.debug("Fetching collection mappings from PostgreSQL...")
    curs.execute(q_detch_colls)
    for currco in curs:
        datasetMap[currco[0]] = currco[1]
    curs.close()

"""Write dataset (name -> clowder_id) mapping to PostgreSQL database"""
def writeDatasetRecordToDatabase(dataset_name, dataset_id):

    q_insert = "INSERT INTO datasets (name, clowder_id) VALUES ('%s', '%s') " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id='%s';" % (
        dataset_name, dataset_id, dataset_id)

    curs = psql_conn.cursor()
    logger.debug("Writing dataset %s to PostgreSQL..." % dataset_name)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Write collection (name -> clowder_id) mapping to PostgreSQL database"""
def writeCollectionRecordToDatabase(collection_name, collection_id):

    q_insert = "INSERT INTO collections (name, clowder_id) VALUES ('%s', '%s') " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id='%s';" % (
        collection_name, collection_id, collection_id)

    curs = psql_conn.cursor()
    logger.debug("Writing collection %s to PostgreSQL..." % collection_name)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Save object into a log file from memory, moving existing file to .backup if it exists"""
def writeStatusToDisk():
    logPath = config["status_log_path"]
    logData = getStatus()
    logger.debug("- writing %s" % os.path.basename(logPath))

    # Create directories if necessary
    dirs = logPath.replace(os.path.basename(logPath), "")
    if not os.path.exists(dirs):
        os.makedirs(dirs)

    # Move existing copy to .backup if it exists
    if os.path.exists(logPath):
        shutil.move(logPath, logPath+".backup")

    f = open(logPath, 'w')
    lockFile(f)
    f.write(json.dumps(logData))
    f.close()


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
            logger.info("%s now being monitored from user %s" % (task['globus_id'], taskUser), extra={
                "globus_id": task['globus_id'],
                "action": "MONITORING NEW TASK",
                "contents": task['contents']
            })

            newTask = {
                "user": taskUser,
                "globus_id": task['globus_id'],
                "contents": task['contents'],
                "received": str(datetime.datetime.now()),
                "completed": None,
                "status": "IN PROGRESS"
            }

            activeTasks[task['globus_id']] = newTask
            writeTaskToDatabase(newTask)

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
            task = readTaskFromDatabase(globusID)
            if task:
                return task, 200
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

            writeTaskToDatabase(task)
            del activeTasks[task['globus_id']]
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
        spaceoverride = req['space_name'] if 'space_name' in req else None

        dsid = fetchDatasetByName(req['dataset'], sess, spaceoverride)
        if dsid:
            logger.info("- adding metadata to dataset "+dsid, extra={
                "dataset_id": dsid,
                "action": "METADATA ADDED",
                "metadata": md
            })
            dsmd = sess.post(config['clowder']['host']+"/api/datasets/"+dsid+"/metadata",
                             headers={'Content-Type':'application/json'},
                             data=json.dumps(md))

            if dsmd.status_code != 200:
                logger.error("- cannot add dataset metadata (%s: %s)" % (dsmd.status_code, dsmd.text))

            return dsmd.status_code
        else:
            return "Dataset could not be accessed", 500

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
        logger.info("- generating auth token for %s" % validUser)
        config['globus']['valid_users'][validUser]['auth_token'] = goauth.get_access_token(
                username=validUser,
                password=config['globus']['valid_users'][validUser]['password']
            ).token

"""Query Globus API to get current transfer status of a given task"""
def getGlobusStatus(task):
    authToken = config['globus']['valid_users'][task['user']]['auth_token']
    api = TransferAPIClient(username=task['user'], goauth=authToken)
    try:
        logger.debug("%s requesting status from Globus" % task['globus_id'])
        status_code, status_message, task_data = api.task(task['globus_id'])
    except (APIError, ClientError) as e:
        try:
            # Refreshing auth tokens and retry
            generateAuthTokens()
            authToken = config['globus']['valid_users'][task['user']]['auth_token']
            api = TransferAPIClient(username=task['user'], goauth=authToken)
            status_code, status_message, task_data = api.task(task['globus_id'])
        except (APIError, ClientError) as e:
            logger.error("%s error checking with Globus for transfer status" % task['globus_id'])
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
        logger.info("%s task complete; notifying Clowder" % task['globus_id'], extra={
            "globus_id": task['globus_id'],
            "action": "NOTIFYING CLOWDER OF COMPLETION"
        })
        clowderHost = config['clowder']['host']
        clowderUser = userMap[globUser]['clowder_user']
        clowderPass = userMap[globUser]['clowder_pass']

        sess = requests.Session()
        sess.auth = (clowderUser, clowderPass)

        # This will be false if any files in the task have errors; task will be revisited
        allDone = True

        # Prepare upload object with all file(s) found
        updatedTask = safeCopy(task)

        spaceoverride = task['contents']['space_id'] if 'space_id' in task['contents'] else None
        for ds in task['contents']:
            filesQueued = []
            fileFormData = []
            datasetMD = None
            datasetMDFile = False
            lastFile = None

            # Assign dataset-level metadata if provided
            if "md" in task['contents'][ds]:
                datasetMD = clean_json_keys(task['contents'][ds]['md'])

            # Add local files to dataset by path
            if 'files' in task['contents'][ds]:
                for f in task['contents'][ds]['files']:
                    fobj = task['contents'][ds]['files'][f]
                    if 'clowder_id' not in fobj or fobj['clowder_id'] == "" or fobj['clowder_id'] == "FILE NOT FOUND":
                        if os.path.exists(fobj['path']):
                            if f.find("metadata.json") == -1:
                                filesQueued.append((fobj['path'], ', "md":'+json.dumps(fobj['md']) if 'md' in fobj else ""))
                                lastFile = f
                            else:
                                datasetMD = clean_json_keys(loadJsonFile(fobj['path']))
                                datasetMDFile = f
                        else:
                            logger.info("%s dataset %s lists nonexistent file: %s" % (task['globus_id'], ds, fobj['path']))
                            updatedTask['contents'][ds]['files'][fobj['name']]['clowder_id'] = "FILE NOT FOUND"
                            writeTaskToDatabase(updatedTask)

            if len(filesQueued)>0 or datasetMD:
                dsid = fetchDatasetByName(ds, sess, spaceoverride)
                dsFileList = fetchDatasetFileList(dsid, sess)
                if dsid:
                    # Only send files not already present in dataset by path
                    for queued in filesQueued:
                        alreadyStored = False
                        for storedFile in dsFileList:
                            if queued[0] == storedFile['filepath']:
                                logger.info("- skipping file %s (already uploaded)" % queued[0])
                                alreadyStored = True
                                break
                        if not alreadyStored:
                            fileFormData.append(("file",'{"path":"%s"%s}' % (queued[0], queued[1])))

                    if datasetMD:
                        # Upload metadata
                        dsmd = sess.post(clowderHost+"/api/datasets/"+dsid+"/metadata",
                                         headers={'Content-Type':'application/json'},
                                         data=json.dumps(datasetMD))

                        if dsmd.status_code != 200:
                            logger.error("- cannot add dataset metadata (%s: %s)" % (dsmd.status_code, dsmd.text))
                            return False
                        else:
                            if datasetMDFile:
                                logger.info("++ added metadata from .json file to dataset %s" % ds, extra={
                                    "dataset_name": ds,
                                    "dataset_id": dsid,
                                    "action": "METADATA ADDED",
                                    "metadata": datasetMD
                                })
                                updatedTask['contents'][ds]['files'][datasetMDFile]['metadata_loaded'] = True
                                updatedTask['contents'][ds]['files'][datasetMDFile]['clowder_id'] = "attached to dataset"
                                writeTaskToDatabase(updatedTask)
                            else:
                                # Remove metadata from activeTasks on success even if file upload fails in next step, so we don't repeat md
                                logger.info("++ added metadata to dataset %s" % ds, extra={
                                    "dataset_name": ds,
                                    "dataset_id": dsid,
                                    "action": "METADATA ADDED",
                                    "metadata": datasetMD
                                })
                                del updatedTask['contents'][ds]['md']
                                writeTaskToDatabase(updatedTask)

                    if len(fileFormData)>0:
                        # Upload collected files for this dataset
                        # Boundary encoding from http://stackoverflow.com/questions/17982741/python-using-reuests-library-for-multipart-form-data
                        logger.info("%s uploading unprocessed files belonging to %s" % (task['globus_id'], ds), extra={
                            "dataset_id": dsid,
                            "dataset_name": ds,
                            "action": "UPLOADING FILES",
                            "filelist": fileFormData
                        })

                        (content, header) = encode_multipart_formdata(fileFormData)
                        fi = sess.post(clowderHost+"/api/uploadToDataset/"+dsid,
                                       headers={'Content-Type':header},
                                       data=content)

                        if fi.status_code != 200:
                            logger.error("- cannot upload files (%s - %s)" % (fi.status_code, fi.text))
                            return False
                        else:
                            loaded = fi.json()
                            if 'ids' in loaded:
                                for fobj in loaded['ids']:
                                    logger.info("++ added file %s" % fobj['name'])
                                    updatedTask['contents'][ds]['files'][fobj['name']]['clowder_id'] = fobj['id']
                                    writeTaskToDatabase(updatedTask)
                            else:
                                logger.info("++ added file %s" % lastFile)
                                updatedTask['contents'][ds]['files'][lastFile]['clowder_id'] = loaded['id']
                                writeTaskToDatabase(updatedTask)
                else:
                    logger.error("- dataset id for %s could not be found/created" % ds)
                    allDone = False
        return allDone
    else:
        logger.error("- cannot find clowder user credentials for Globus user %s" % globUser)
        return False

"""Continually check Globus API for task updates"""
def globusMonitorLoop():
    global unprocessedTasks

    authWait = 0
    globWait = 0
    while True:
        time.sleep(1)
        authWait += 1
        globWait += 1

        # Check with Globus for any status updates on monitored tasks
        if globWait >= config['globus']['transfer_update_frequency_secs']:
            logger.info("- checking for Globus updates")
            # Use copy of task list so it doesn't change during iteration
            currentActiveTasks = safeCopy(activeTasks)
            for globusID in currentActiveTasks:
                task = activeTasks[globusID]
                globusStatus = getGlobusStatus(task)
                logger.info("%s status received: %s" % (globusID, globusStatus), extra={
                    "globus_id": globusID,
                    "status": globusStatus,
                    "action": "STATUS UPDATED"
                })

                # If this isn't done yet, leave the task active so we can try again next time
                if globusStatus in ["SUCCEEDED", "FAILED"]:
                    # Update task parameters
                    task['status'] = globusStatus
                    task['completed'] = str(datetime.datetime.now())
                    for ds in task['contents']:
                        if 'files' in task['contents'][ds]:
                            for f in task['contents'][ds]['files']:
                                fobj = task['contents'][ds]['files'][f]
                                fobj['path'] = os.path.join(config['globus']['incoming_files_path'], fobj['path'])

                    # Notify Clowder to process file if transfer successful
                    if globusStatus == "SUCCEEDED":
                        unprocessedTasks.append(globusID)

                    writeTaskToDatabase(task)
                    del activeTasks[globusID]

            logger.debug("- done checking for Globus updates")

            globWait = 0
            writeStatusToDisk()

        # Refresh auth tokens periodically
        if authWait >= config['globus']['authentication_refresh_frequency_secs']:
            generateAuthTokens()
            authWait = 0

"""Work on completed Globus transfers to process them into Clowder"""
def clowderSubmissionLoop():
    global unprocessedTasks

    clowderWait = config['clowder']['globus_processing_frequency'] - 1
    while True:
        time.sleep(1)
        clowderWait += 1

        # Check with Globus for any status updates on monitored tasks
        if clowderWait >= config['clowder']['globus_processing_frequency']:
            logger.info("- checking unprocessed tasks")
            toHandle = safeCopy(unprocessedTasks)

            for globusID in toHandle:
                task = readTaskFromDatabase(globusID)
                if task:
                    clowderDone = notifyClowderOfCompletedTask(task)
                    if clowderDone:
                        logger.info("%s task successfully processed!" % globusID, extra={
                            "globus_id": globusID,
                            "action": "PROCESSING COMPLETE"
                        })
                        unprocessedTasks.remove(globusID)
                        task['status'] = 'PROCESSED'
                        writeTaskToDatabase(task)
                    else:
                        logger.error("%s not successfully sent" % globusID)
                else:
                    logger.error("%s unprocessed task record not found in completed tasks" % globusID)

            clowderWait = 0

if __name__ == '__main__':
    # Try to load custom config file, falling back to default values where not overridden
    config = loadJsonFile(os.path.join(rootPath, "config_default.json"))
    if os.path.exists(os.path.join(rootPath, "data/config_custom.json")):
        print("...loading configuration from config_custom.json")
        config = updateNestedDict(config, loadJsonFile(os.path.join(rootPath, "data/config_custom.json")))
    else:
        print("...no custom configuration file found. using default values")

    # Initialize logger handlers
    with open(os.path.join(rootPath,"config_logging.json"), 'r') as f:
        log_config = json.load(f)
        log_config['handlers']['file']['filename'] = config["log_path"]
        logging.config.dictConfig(log_config)
    logger = logging.getLogger('gantry')

    psql_conn = connectToPostgres()
    readRecordsFromDatabase()
    activeTasks = readTasksByStatus("IN PROGRESS")
    unprocessedTasks = readTasksByStatus("SUCCEEDED")
    generateAuthTokens()

    logger.info("- initializing services")
    # Create thread for service to begin monitoring
    thread.start_new_thread(globusMonitorLoop, ())
    logger.info("*** Service now monitoring Globus tasks ***")
    thread.start_new_thread(clowderSubmissionLoop, ())
    logger.info("*** Service now waiting to process completed tasks into Clowder ***")

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    logger.info("*** API now listening on %s:%s ***" % (config['api']['ip_address'], apiPort))
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

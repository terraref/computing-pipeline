#!/usr/bin/python

""" GLOBUS UPLOADER SERVICE
    This will query Postgres for Globus tasks that are marked as
    'SUCCEEDED' but not yet 'PROCESSED' and notify Clowder to
    process the contents.
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

    logger.info("Connected to Postgres")
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
    #logger.debug("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Fetch all Globus tasks with a particular status"""
def getNextUnprocessedTask():
    q_fetch = "SELECT * FROM globus_tasks WHERE status = 'SUCCEEDED' order by completed ASC limit 1"
    nextTask = None

    curs = psql_conn.cursor()
    logger.debug("Fetching next unprocessed task from PostgreSQL...")
    curs.execute(q_fetch)
    for result in curs:
        nextTask = {
            "globus_id": result[0],
            "status": result[1],
            "received": result[2],
            "completed": result[3],
            "user": result[4],
            "contents": result[5]
        }
    curs.close()

    return nextTask

"""Fetch mappings of dataset/collection name to Clowder ID"""
def readRecordsFromDatabase():
    q_fetch_datas = "SELECT * FROM datasets;"
    q_detch_colls = "SELECT * FROM collections;"

    curs = psql_conn.cursor()
    logger.debug("Fetching dataset mappings from PostgreSQL...")
    curs.execute(q_fetch_datas)
    for currds in curs:
        datasetMap[currds[0]] = currds[1]

    logger.debug("Fetching collection mappings from PostgreSQL...")
    curs.execute(q_detch_colls)
    for currco in curs:
        collectionMap[currco[0]] = currco[1]
    curs.close()

"""Write dataset (name -> clowder_id) mapping to PostgreSQL database"""
def writeDatasetRecordToDatabase(dataset_name, dataset_id):

    q_insert = "INSERT INTO datasets (name, clowder_id) VALUES ('%s', '%s') " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id='%s';" % (
                   dataset_name, dataset_id, dataset_id)

    curs = psql_conn.cursor()
    #logger.debug("Writing dataset %s to PostgreSQL..." % dataset_name)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Write collection (name -> clowder_id) mapping to PostgreSQL database"""
def writeCollectionRecordToDatabase(collection_name, collection_id):

    q_insert = "INSERT INTO collections (name, clowder_id) VALUES ('%s', '%s') " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id='%s';" % (
                   collection_name, collection_id, collection_id)

    curs = psql_conn.cursor()
    #logger.debug("Writing collection %s to PostgreSQL..." % collection_name)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()


# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
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
                                if 'md' in fobj:
                                    # Use [1,-1] to avoid json.dumps wrapping quotes
                                    # Replace \" with " to avoid json.dumps escaping quotes
                                    mdstr = ', "md":' + json.dumps(fobj['md'])[1:-1].replace('\\"', '"')
                                else:
                                    mdstr = ""
                                filesQueued.append((fobj['path'], mdstr))
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

"""Work on completed Globus transfers to process them into Clowder"""
def clowderSubmissionLoop():
    clowderWait = config['clowder']['globus_processing_frequency'] - 1
    while True:
        time.sleep(1)
        clowderWait += 1

        # Check with Globus for any status updates on monitored tasks
        if clowderWait >= config['clowder']['globus_processing_frequency']:
            task = getNextUnprocessedTask()

            if task:
                globusID = task['globus_id']
                clowderDone = notifyClowderOfCompletedTask(task)
                if clowderDone:
                    logger.info("%s task successfully processed!" % globusID, extra={
                        "globus_id": globusID,
                        "action": "PROCESSING COMPLETE"
                    })
                    task['status'] = 'PROCESSED'
                    writeTaskToDatabase(task)
                else:
                    logger.error("%s not successfully sent" % globusID)

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
        log_config['handlers']['file']['filename'] = config["uploader_log_path"]
        logging.config.dictConfig(log_config)
    logger = logging.getLogger('gantry')

    psql_conn = connectToPostgres()
    readRecordsFromDatabase()

    logger.info("- initializing service")
    # Create thread for service to begin monitoring
    thread.start_new_thread(clowderSubmissionLoop, ())
    logger.info("*** Service now waiting to process completed tasks into Clowder ***")

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
def fetchDatasetByName(datasetName, sess, spaceOverrideId=None):
    if datasetName in datasetMap:
        return datasetMap[datasetName]
    else:
        # Get names of collection hierarchy
        if datasetName.find(" - ") > -1:
            # e.g. "co2Sensor - 2016-12-25" or "VNIR - 2016-12-25__12-32-42-123"
            c_sensor = datasetName.split(" - ")[0]
            c_date = datasetName.split(" - ")[1]
            c_year = c_sensor + " - " + c_date.split('-')[0]
            c_month = c_year+"-"+c_date.split('-')[1]
            if c_date.find("__") == -1:
                # If we only have a date and not a timestamp, don't create date collection
                c_date = None
            else:
                c_date = c_sensor + " - " + c_date.split("__")[0]
        else:
            c_sensor, c_date, c_year, c_month = None, None, None, None

        id_sensor = fetchCollectionByName(c_sensor, sess, spaceOverrideId) if c_sensor else None
        id_date = fetchCollectionByName(c_date, sess, spaceOverrideId) if c_date else None
        id_year = fetchCollectionByName(c_year, sess, spaceOverrideId) if c_year else None
        id_month = fetchCollectionByName(c_month, sess, spaceOverrideId) if c_month else None
        if id_year and id_year['created']:
            associateChildCollection(id_sensor['id'], id_year['id'], sess)
        if id_month and id_month['created']:
            associateChildCollection(id_year['id'], id_month['id'], sess)
        if id_date and id_date['created']:
            associateChildCollection(id_month['id'], id_date['id'], sess)

        if not spaceOverrideId and config['clowder']['primary_space'] != "":
            spaceOverrideId = config['clowder']['primary_space']
        spaceId = ', "space": ["%s"]' % spaceOverrideId if spaceOverrideId else ''

        if id_date:
            dataObj = '{"name": "%s", "collection": ["%s"]%s}' % (datasetName, id_date['id'], spaceId)
        else:
            dataObj = '{"name": "%s", "collection": ["%s"]%s}' % (datasetName, id_month['id'], spaceId)
        ds = sess.post(config['clowder']['host']+"/api/datasets/createempty",
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
def fetchCollectionByName(collectionName, requestsSession, spaceOverrideId=None):
    if collectionName in collectionMap:
        return {
            "id": collectionMap[collectionName],
            "created": False
        }
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
            if not spaceOverrideId and config['clowder']['primary_space'] != "":
                spaceOverrideId = config['clowder']['primary_space']

            if spaceOverrideId:
                requestsSession.post(config['clowder']['host']+"/api/spaces/%s/addCollectionToSpace/%s" %
                                     (spaceOverrideId, collid))
            return {
                "id": collid,
                "created": True
            }
        else:
            logger.error("- cannot create collection (%s: %s)" % (coll.status_code, coll.text))
            return None

"""Add child collection to parent collection"""
def associateChildCollection(parentId, childId, requestsSession):
    requestsSession.post(config['clowder']['host']+"/api/collections/%s/addSubCollection/%s" %
                         (parentId, childId))


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
    psql_db = config['postgres']['database']
    psql_user = config['postgres']['username']
    psql_pass = config['postgres']['password']

    try:
        conn = psycopg2.connect(dbname=psql_db, user=psql_user, password=psql_pass)
    except:
        # Attempt to create database if not found
        conn = psycopg2.connect(dbname='postgres')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        curs.execute('CREATE DATABASE %s;' % psql_db)
        curs.close()
        conn.commit()
        conn.close()

        conn = psycopg2.connect(dbname=psql_db, user=psql_user, password=psql_pass)
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
            lastFileKey = None

            # Assign dataset-level metadata if provided
            if "md" in task['contents'][ds]:
                datasetMD = clean_json_keys(task['contents'][ds]['md'])

            # Add local files to dataset by path
            if 'files' in task['contents'][ds]:
                for fkey in task['contents'][ds]['files']:
                    fobj = task['contents'][ds]['files'][fkey]
                    if 'clowder_id' not in fobj or fobj['clowder_id'] == "" or fobj['clowder_id'] == "FILE NOT FOUND":
                        if os.path.exists(fobj['path']):
                            if fobj['name'].find("metadata.json") == -1:
                                if 'md' in fobj:
                                    # Use [1,-1] to avoid json.dumps wrapping quotes
                                    # Replace \" with " to avoid json.dumps escaping quotes
                                    mdstr = ', "md":' + json.dumps(fobj['md'])[1:-1].replace('\\"', '"')
                                else:
                                    mdstr = ""
                                filesQueued.append((fobj['path'], mdstr))
                                lastFile = fobj['name']
                                lastFileKey = fkey
                            else:
                                datasetMD = clean_json_keys(loadJsonFile(fobj['path']))
                                datasetMDFile = fkey
                        else:
                            logger.info("%s dataset %s lists nonexistent file: %s" % (task['globus_id'], ds, fobj['path']))
                            updatedTask['contents'][ds]['files'][fkey]['clowder_id'] = "FILE NOT FOUND"
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
                                    for fkey in updatedTask['contents'][ds]['files']:
                                        if updatedTask['contents'][ds]['files'][fkey]['name'] == fobj['name']:
                                            updatedTask['contents'][ds]['files'][fkey]['clowder_id'] = fobj['id']
                                            break
                                    writeTaskToDatabase(updatedTask)
                            else:
                                logger.info("++ added file %s" % lastFile)
                                updatedTask['contents'][ds]['files'][lastFileKey]['clowder_id'] = loaded['id']
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

            while task:
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

                task = getNextUnprocessedTask()

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
        main_log_file = os.path.join(config["log_path"], "log_uploader.txt")
        log_config['handlers']['file']['filename'] = main_log_file
        if not os.path.exists(config["log_path"]):
            os.makedirs(config["log_path"])
        if not os.path.isfile(main_log_file):
            open(main_log_file, 'a').close()
        logging.config.dictConfig(log_config)
    logger = logging.getLogger('gantry')

    psql_conn = connectToPostgres()
    readRecordsFromDatabase()

    logger.info("- initializing service")
    # Create thread for service to begin monitoring
    clowderSubmissionLoop()

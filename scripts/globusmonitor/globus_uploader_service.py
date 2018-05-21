#!/usr/bin/python

""" GLOBUS UPLOADER SERVICE
    This will query Postgres for Globus tasks that are marked as
    'SUCCEEDED' but not yet 'PROCESSED' and notify Clowder to
    process the contents.
"""

import os, shutil, json, time, datetime, thread, copy, atexit, collections, fcntl
import logging, logging.config, logstash
import requests
import signal
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from io import BlockingIOError
from urllib3.filepost import encode_multipart_formdata

from pyclowder.datasets import download_metadata
from terrautils.metadata import clean_metadata
from terrautils.extractors import build_dataset_hierarchy

rootPath = "/home/globusmonitor"

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

# Store any PENDING task ID here
current_task = None

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
    psql_db = os.getenv("POSTGRES_DATABASE", config['postgres']['database'])
    psql_host = os.getenv("POSTGRES_HOST", config['postgres']['host'])
    psql_user = os.getenv("POSTGRES_USER", config['postgres']['username'])
    psql_pass = os.getenv("POSTGRES_PASSWORD", config['postgres']['password'])

    try:
        conn = psycopg2.connect(dbname=psql_db, user=psql_user, password=psql_pass, host=psql_host)
    except:
        # Attempt to create database if not found
        conn = psycopg2.connect(dbname='postgres', host=psql_host)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        curs.execute('CREATE DATABASE %s;' % psql_db)
        curs.close()
        conn.commit()
        conn.close()

        conn = psycopg2.connect(dbname=psql_db, user=psql_user, password=psql_pass, host=psql_host)
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
def getNextUnprocessedTask(status="SUCCEEDED", reverse=False):

    # Use Common Table Expression to update status to PENDING and return row at the same time
    if reverse:
        q_fetch = "WITH cte AS (SELECT globus_id FROM globus_tasks where STATUS = '%s' ORDER BY completed DESC LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE globus_tasks gt  SET status = 'PENDING' FROM cte WHERE gt.globus_id = cte.globus_id RETURNING *" %status

    else:
        q_fetch = "WITH cte AS (SELECT globus_id FROM globus_tasks where STATUS = '%s' ORDER BY completed ASC LIMIT 1 FOR UPDATE SKIP LOCKED) UPDATE globus_tasks gt  SET status = 'PENDING' FROM cte WHERE gt.globus_id = cte.globus_id RETURNING *" %status

    nextTask = None

    try:
        curs = psql_conn.cursor()
        logger.debug("Fetching next %s task from PostgreSQL..." % status)
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
    except Exception as e:
        logger.error("Exception fetching task: %s" % str(e))

    if nextTask:
        current_task = nextTask['globus_id']
        logger.debug("Found task %s [%s]" % (nextTask['globus_id'], nextTask['completed']))
    else:
        current_task = None
        logger.debug("No task found.")
    return nextTask

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

"""Remove PENDING status from any ongoing processing if this is killed"""
def gracefulExit():
    if current_task:
        curs = psql_conn.cursor()
        query = "update globus_tasks set STATUS='SUCCEEDED' where globus_id = %s;" % current_task
        logger.debug("Gracefully resolving PENDING for %s" % current_task)
        curs.execute(query)
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
        clowder_host = config['clowder']['host']
        clowder_key  = config['clowder']['secret_key']
        clowder_user = userMap[globUser]['clowder_user']
        clowder_pass = userMap[globUser]['clowder_pass']
        clowder_id = userMap[globUser]['clowder_id']
        clowder_context = userMap[globUser]['context']

        sess = requests.Session()
        sess.auth = (clowder_user, clowder_pass)

        # Response can be OK, RETRY or ERROR
        response = "OK"

        # Prepare upload object with all file(s) found
        updatedTask = safeCopy(task)

        space_id = task['contents']['space_id'] if 'space_id' in task['contents'] else config['clowder']['primary_space']
        for ds in task['contents']:
            # Skip any unexpected files at root level, e.g.
            #   /home/clowder/sites/ua-mac/raw_data/GetFluorescenceValues.m
            #   /home/clowder/sites/ua-mac/raw_data/irrigation/2017-06-04/@Recycle/flowmetertotals_March-2017.csv",
            if ds in ["LemnaTec - MovingSensor"] or ds.find("@Recycle") > -1:
                continue
                
            filesQueued = []
            fileFormData = []
            datasetMD = None
            datasetMDFile = False
            lastFile = None
            lastFileKey = None
            sensorname = ds.split(" - ")[0]

            logger.info("%s -- Processing [%s]" % (task['globus_id'], ds))

            # Assign dataset-level metadata if provided
            if "md" in task['contents'][ds]:
                datasetMD = task['contents'][ds]['md']

            # Add local files to dataset by path
            if 'files' in task['contents'][ds]:
                for fkey in task['contents'][ds]['files']:
                    fobj = task['contents'][ds]['files'][fkey]
                    if 'clowder_id' not in fobj or fobj['clowder_id'] == "":
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
                                try:
                                    datasetMD = loadJsonFile(fobj['path'])
                                    datasetMDFile = fkey
                                except:
                                    logger.error("[%s] could not decode JSON from %s" % (ds, fobj['path']))
                                    updatedTask['contents'][ds]['files'][fkey]['clowder_id'] = "FILE NOT FOUND"
                                    updatedTask['contents'][ds]['files'][fkey]['error'] = "Failed to load JSON"
                                    writeTaskToDatabase(updatedTask)
                                    if response == "OK": response = "ERROR" # Don't overwrite a RETRY
                        else:
                            logger.error("[%s] file not found: %s" % (ds, fobj['path']))
                            updatedTask['contents'][ds]['files'][fkey]['clowder_id'] = "FILE NOT FOUND"
                            updatedTask['contents'][ds]['files'][fkey]['error'] = "File not found"
                            writeTaskToDatabase(updatedTask)
                            if response == "OK": response = "ERROR" # Don't overwrite a RETRY

            if len(filesQueued)>0 or datasetMD:
                # Try to clean metadata first
                if datasetMD:
                    cleaned_dsmd = None
                    try:
                        cleaned_dsmd = clean_metadata(datasetMD, sensorname)
                    except Exception as e:
                        logger.error("[%s] could not clean md: %s" % (ds, str(e)))
                        task['contents'][ds]['error'] = "Could not clean metadata: %s" % str(e)
                        # TODO: possible this could be recoverable with more info from clean_metadata
                        if response == "OK": response = "ERROR" # Don't overwrite a RETRY

                if ds.find(" - ") > -1:
                    # e.g. "co2Sensor - 2016-12-25" or "VNIR - 2016-12-25__12-32-42-123"
                    c_sensor = ds.split(" - ")[0]
                    c_date  = ds.split(" - ")[1]
                    c_year  = c_date.split('-')[0]
                    c_month = c_date.split('-')[1]
                    if c_date.find("__") == -1:
                        # If we only have a date and not a timestamp, don't create date collection
                        c_date = None
                    else:
                        c_date = c_date.split("__")[0].split("-")[2]
                else:
                    c_sensor, c_date, c_year, c_month = ds, None, None, None

                # Get dataset from clowder, or create & associate with collections
                try:
                    hierarchy_host = clowder_host + ("/" if not clowder_host.endswith("/") else "")
                    dsid = build_dataset_hierarchy(hierarchy_host, clowder_key, clowder_user, clowder_pass, space_id,
                                                   c_sensor, c_year, c_month, c_date, ds)
                    logger.info("   [%s] id: %s" % (ds, dsid))
                except Exception as e:
                    logger.error("[%s] could not build hierarchy: %s" % (ds, str(e)))
                    task['contents'][ds]['retry'] = "Count not build dataset hierarchy: %s" % str(e)
                    response = "RETRY"
                    continue

                if dsid:
                    dsFileList = fetchDatasetFileList(dsid, sess)
                    # Only send files not already present in dataset by path
                    for queued in filesQueued:
                        alreadyStored = False
                        for storedFile in dsFileList:
                            if queued[0] == storedFile['filepath']:
                                logger.info("   skipping file %s (already uploaded)" % queued[0])
                                alreadyStored = True
                                break
                        if not alreadyStored:
                            fileFormData.append(("file",'{"path":"%s"%s}' % (queued[0], queued[1])))

                    if datasetMD and cleaned_dsmd:
                        # Check for existing metadata from the site user
                        alreadyAttached = False
                        md_existing = download_metadata(None, hierarchy_host, clowder_key, dsid)
                        for mdobj in md_existing:
                            if 'agent' in mdobj and 'user_id' in mdobj['agent']:
                                if mdobj['agent']['user_id'] == "https://terraref.ncsa.illinois.edu/clowder/api/users/%s" % clowder_id:
                                    logger.info("   skipping metadata (already attached)")
                                    alreadyAttached = True
                                    break
                        if not alreadyAttached:
                            md = {
                                "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld",
                                             {"@vocab": clowder_context}],
                                "content": cleaned_dsmd,
                                "agent": {
                                    "@type": "cat:user",
                                    "user_id": "https://terraref.ncsa.illinois.edu/clowder/api/users/%s" % clowder_id
                                }
                            }
                            dsmd = sess.post(clowder_host+"/api/datasets/"+dsid+"/metadata.jsonld",
                                             headers={'Content-Type':'application/json'},
                                             data=json.dumps(md))

                            if dsmd.status_code in [500, 502, 504]:
                                logger.error("[%s] failed to attach metadata (%s: %s)" % (ds, dsmd.status_code, dsmd.text))
                                updatedTask['contents'][ds]['files'][datasetMDFile]['retry'] = "%s: %s" % (dsmd.status_code, dsmd.text)
                                response = "RETRY"
                            elif dsmd.status_code != 200:
                                logger.error("[%s] failed to attach metadata (%s: %s)" % (ds, dsmd.status_code, dsmd.text))
                                updatedTask['contents'][ds]['files'][datasetMDFile]['error'] = "%s: %s" % (dsmd.status_code, dsmd.text)
                                response = "ERROR"
                            else:
                                if datasetMDFile:
                                    logger.info("   [%s] added metadata from .json file" % ds, extra={
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
                                    logger.info("   [%s] added metadata" % ds, extra={
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
                        logger.info("   [%s] uploading unprocessed files" % ds, extra={
                            "dataset_id": dsid,
                            "dataset_name": ds,
                            "action": "UPLOADING FILES",
                            "filelist": fileFormData
                        })

                        (content, header) = encode_multipart_formdata(fileFormData)
                        fi = sess.post(clowder_host+"/api/uploadToDataset/"+dsid,
                                       headers={'Content-Type':header},
                                       data=content)

                        if fi.status_code in [500, 502, 504]:
                            logger.error("[%s] failed to attach files (%s: %s)" % (ds, fi.status_code, fi.text))
                            updatedTask['contents'][ds]['files'][datasetMDFile]['retry'] = "%s: %s" % (fi.status_code, fi.text)
                            response = "RETRY"
                        if fi.status_code != 200:
                            logger.error("[%s] failed to attach files (%s: %s)" % (ds, fi.status_code, fi.text))
                            updatedTask['contents'][ds]['files'][datasetMDFile]['error'] = "%s: %s" % (fi.status_code, fi.text)
                            response = "ERROR"
                        else:
                            loaded = fi.json()
                            if 'ids' in loaded:
                                for fobj in loaded['ids']:
                                    logger.info("   [%s] added file %s" % (ds, fobj['name']))
                                    for fkey in updatedTask['contents'][ds]['files']:
                                        if updatedTask['contents'][ds]['files'][fkey]['name'] == fobj['name']:
                                            updatedTask['contents'][ds]['files'][fkey]['clowder_id'] = fobj['id']
                                            # remove any previous retry/error messages
                                            if 'retry' in updatedTask['contents'][ds]['files'][fkey]:
                                                del(updatedTask['contents'][ds]['files'][fkey]['retry'])
                                            if 'error' in updatedTask['contents'][ds]['files'][fkey]:
                                                del(updatedTask['contents'][ds]['files'][fkey]['error'])
                                            break
                                    writeTaskToDatabase(updatedTask)
                            else:
                                logger.info("   [%s] added file %s" % (ds, lastFile))
                                updatedTask['contents'][ds]['files'][lastFileKey]['clowder_id'] = loaded['id']
                                # remove any previous retry/error messages
                                if 'retry' in updatedTask['contents'][ds]['files'][lastFileKey]:
                                    del(updatedTask['contents'][ds]['files'][lastFileKey]['retry'])
                                if 'error' in updatedTask['contents'][ds]['files'][lastFileKey]:
                                    del(updatedTask['contents'][ds]['files'][lastFileKey]['error'])
                                writeTaskToDatabase(updatedTask)

        return response
    else:
        logger.error("%s task: no credentials for Globus user %s" % (task['globus_id'], globUser))
        return "ERROR"

"""Work on completed Globus transfers to process them into Clowder"""
def clowderSubmissionLoop():
    clowderWait = config['clowder']['globus_processing_frequency'] - 1
    while True:
        time.sleep(1)
        clowderWait += 1

        # Check with Globus for any status updates on monitored tasks
        if clowderWait >= config['clowder']['globus_processing_frequency']:
            # First handle all regular tasks
            task = getNextUnprocessedTask()
            while task:
                globusID = task['globus_id']
                try: 
                    clowderDone = notifyClowderOfCompletedTask(task)
                    if clowderDone == "OK":
                        logger.info("%s task successfully processed!" % globusID, extra={
                            "globus_id": globusID,
                            "action": "PROCESSING COMPLETE"
                        })
                        task['status'] = 'PROCESSED'
                        writeTaskToDatabase(task)
                    else:
                        logger.error("%s not successfully processed; marking %s" % (globusID, clowderDone))
                        task['status'] = clowderDone
                        writeTaskToDatabase(task)
                except SocketError as e:
                    if e.errno != errno.ECONNRESET:
                        logger.error("Exception processing task %s; marking ERROR (%s)" % (globusID, str(e)))
                        task['status'] = 'ERROR'
                        writeTaskToDatabase(task)
                    else:
                        logger.error("Connection reset on %s; marking RETRY (%s)" % (globusID, str(e)))
                        task['status'] = 'RETRY'
                        writeTaskToDatabase(task)
                except ConnectionError as e:
                    logger.error("Connection error on %s; marking RETRY (%s)" % (globusID, str(e)))
                    task['status'] = 'RETRY'
                    writeTaskToDatabase(task)
                except Exception as e:
                    logger.error("Exception processing task %s; marking ERROR (%s)" % (globusID, str(e)))
                    task['status'] = 'ERROR'
                    writeTaskToDatabase(task)
   
                task = getNextUnprocessedTask()
  
            # Next attempt to handle any ERROR tasks a second time
            task = getNextUnprocessedTask("RETRY", reverse=True)
            while task:
                globusID = task['globus_id']
                try: 
                    clowderDone = notifyClowderOfCompletedTask(task)
                    if clowderDone == "OK":
                        logger.info("%s task successfully processed!" % globusID, extra={
                            "globus_id": globusID,
                            "action": "PROCESSING COMPLETE"
                        })
                        task['status'] = 'PROCESSED'
                        writeTaskToDatabase(task)
                    else:
                        logger.error("%s not successfully processed; marking %s" % (globusID, clowderDone))
                        task['status'] = clowderDone
                        writeTaskToDatabase(task)
                except SocketError as e:
                    if e.errno != errno.ECONNRESET:
                        logger.error("Exception processing task %s; marking ERROR (%s)" % (globusID, str(e)))
                        task['status'] = 'ERROR'
                        writeTaskToDatabase(task)
                    else:
                        logger.error("Connection reset on %s; marking RETRY (%s)" % (globusID, str(e)))
                        task['status'] = 'RETRY'
                        writeTaskToDatabase(task)
                except ConnectionError as e:
                    logger.error("Connection error on %s; marking RETRY (%s)" % (globusID, str(e)))
                    task['status'] = 'RETRY'
                    writeTaskToDatabase(task)
                except Exception as e:
                    logger.error("Exception processing task %s; marking ERROR" % globusID, str(e))
                    task['status'] = 'ERROR'
                    writeTaskToDatabase(task)
 
                task = getNextUnprocessedTask("RETRY", reverse=True)

            clowderWait = 0

if __name__ == '__main__':
    signal.signal(signal.SIGINT, gracefulExit)

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

    logger.info("- initializing service")
    # Create thread for service to begin monitoring
    clowderSubmissionLoop()

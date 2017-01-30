#!/usr/bin/env python

""" GLOBUS MANAGER SERVICE
    This will continually check Globus transfers from Postgres for completion
    status.

    The service will check with Globus directly to mark the transfers as complete
    and purge them from the active list, and check with NCSA to make sure Clowder
    is made aware of each transfer (whether complete or not).
"""

import os, shutil, json, time, datetime, thread, copy, subprocess, atexit, collections, fcntl, re, gzip, pwd
import logging, logging.config, logstash
import requests
from io import BlockingIOError
from flask import Flask, request, Response
from flask.ext import restful
from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, ClientError, goauth

rootPath = "/home/gantry"

config = {}

"""activeTasks tracks Globus transfers is of the format:
{"globus_id": {
    "globus_id":                globus job ID of upload
    "contents": {...},          a pendingTransfers object that was sent (see above)
    "started":                  timestamp when task was sent to Globus
    "completed":                timestamp when task was completed (including errors and cancelled tasks)
    "status":                   can be "IN PROGRESS", "DONE", "ABORTED", "ERROR"
}, {...}, {...}, ...}"""
activeTasks = {}

app = Flask(__name__)
api = restful.Api(app)

# ----------------------------------------------------------
# SHARED UTILS
# ----------------------------------------------------------
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
        logger.error("- unable to open or parse JSON from %s" % filename)
        return {}

"""Load active or pending tasks from file into memory"""
def loadTasksFromDisk(filePath, emptyVal="{}"):
    # Prefer to load from primary file, try to use backup if primary is missing
    if not os.path.exists(filePath):
        if os.path.exists(filePath+".backup"):
            logger.info("- loading data from %s.backup" % filePath)
            shutil.copyfile(filePath+".backup", filePath)
        else:
            # Create an empty file if primary+backup don't exist
            f = open(filePath, 'w')
            f.write(emptyVal)
            f.close()
    else:
        logger.info("- loading data from %s" % filePath)

    return loadJsonFile(filePath)

"""Write active or pending tasks from memory into file"""
def writeTasksToDisk(filePath, taskObj):
    # Write current file to backup location before writing current file
    logger.debug("- writing %s" % os.path.basename(filePath))

    # Create directories if necessary
    dirs = filePath.replace(os.path.basename(filePath), "")
    if not os.path.exists(dirs):
        os.makedirs(dirs)

    if os.path.exists(filePath):
        shutil.move(filePath, filePath+".backup")

    f = open(filePath, 'w')
    lockFile(f)
    f.write(json.dumps(taskObj))
    f.close()

"""Write a completed task onto disk in appropriate folder hierarchy"""
def writeCompletedTransferToDisk(transfer):
    completedPath = config['completed_tasks_path']
    taskID = transfer['globus_id']

    # e.g. TaskID "eaca1f1a-d400-11e5-975b-22000b9da45e"
    #   = <completedPath>/ea/ca/1f/1a/eaca1f1a-d400-11e5-975b-22000b9da45e.json

    # Create path if necessary
    logPath = os.path.join(completedPath, taskID[:2], taskID[2:4], taskID[4:6], taskID[6:8])
    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # Write to json file with task ID as filename
    dest = os.path.join(logPath, taskID+".json")
    logger.info("%s complete (%s)" % (taskID, dest), extra={
        "globus_id": taskID,
        "action": "WRITING TO COMPLETED (GANTRY)"
    })
    f = open(dest, 'w')
    f.write(json.dumps(transfer))
    f.close()

"""Create symlink to src file in destPath"""
def createLocalSymlink(srcPath, destPath):
    try:
        dest_dirs = destPath.replace(os.path.basename(destPath), "")
        if not os.path.isdir(dest_dirs):
            os.makedirs(dest_dirs)

        #logger.info("- creating symlink to %s in %s" % (srcPath, destPath))
        os.symlink(srcPath, destPath)

        # Change original file to make it immutable
        srcPath = srcPath.replace(config['globus']['source_path'], config['gantry']['incoming_files_path'])
        #logger.debug("- chattr for %s" % srcPath)
        #subprocess.call(["chattr", "-i", srcPath])
    except OSError as e:
        if e.errno != 17:
            logger.error("- error on symlink to %s (%s - %s)" % (destPath, e.errno, e.strerror))
        return

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
    ct_tasks = "CREATE TABLE globus_tasks (globus_id TEXT PRIMARY KEY NOT NULL, status TEXT NOT NULL, " \
               "started TEXT NOT NULL, completed TEXT, " \
               "file_count INT, bytes BIGINT, globus_user TEXT, contents JSON);"

    # Index creation queries
    ix_tasks = "CREATE UNIQUE INDEX globus_idx ON globus_tasks (globus_id);"

    # Execute each query
    curs = db_connection.cursor()
    logger.info("Creating PostgreSQL tables...")
    curs.execute(ct_tasks)
    logger.info("Creating PostgreSQL indexes...")
    curs.execute(ix_tasks)
    curs.close()
    db_connection.commit()

    logger.info("PostgreSQL initialization complete.")

"""Fetch a Globus task from PostgreSQL"""
def readTaskFromDatabase(globus_id):
    q_fetch = "SELECT globus_id, status, started, completed, globus_user, " \
              "file_count, bytes, contents FROM globus_tasks WHERE globus_id = '%s'" % globus_id

    curs = psql_conn.cursor()
    logger.debug("Fetching task %s from PostgreSQL..." % globus_id)
    curs.execute(q_fetch)
    result = curs.fetchone()
    curs.close()

    if result:
        return {
            "globus_id": result[0],
            "status": result[1],
            "started": result[2],
            "completed": result[3],
            "user": result[4],
            "file_count": result[5],
            "bytes": result[6],
            "contents": result[7]
        }
    else:
        logger.debug("Task %s not found in PostgreSQL" % globus_id)
        return None

"""Write a Globus task into PostgreSQL, insert/update as needed"""
def writeTaskToDatabase(task):
    gid = task['globus_id']
    stat = task['status']
    start = task['started']
    comp = task['completed']
    guser = task['user']
    filecount = int(task['file_count']) if 'file_count' in task else -1
    bytecount = int(task['bytes']) if 'bytes' in task else -1
    jbody = json.dumps(task['contents'])

    # Attempt to insert, update if globus ID already exists
    q_insert = "INSERT INTO globus_tasks (globus_id, status, started, completed, globus_user, file_count, bytes, contents) " \
               "VALUES ('%s', '%s', '%s', '%s', '%s', %s, %s, '%s') " \
               "ON CONFLICT (globus_id) DO UPDATE " \
               "SET status='%s', received='%s', completed='%s', globus_user='%s', file_count=%s, bytes=%s, contents='%s';" % (
                   gid, stat, start, comp, guser, filecount, bytecount, jbody, stat, start, comp, guser, filecount, bytecount, jbody)

    curs = psql_conn.cursor()
    #logger.debug("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Fetch all Globus tasks with a particular status"""
def getNextUnnotifiedTask():
    q_fetch = "SELECT * FROM globus_tasks WHERE status = 'SUCCEEDED' OR status = 'CREATED' order by completed ASC limit 1"
    nextTask = None

    curs = psql_conn.cursor()
    logger.debug("Fetching next unnotified task from PostgreSQL...")
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

"""Fetch all Globus tasks with a particular status"""
def readTasksByStatus(status, id_only=False):
    """
        CREATED (initialized transfer; not yet notified NCSA side)
    IN PROGRESS (notified of transfer; but not yet verified complete)
         FAILED (Globus could not complete; no longer attempting to complete)
        DELETED (manually via api below)
      SUCCEEDED (verified complete; not yet notified NCSA side)
       NOTIFIED (verified complete; not yet uploaded into Clowder)
      PROCESSED (complete & uploaded into Clowder)
    """
    if id_only:
        q_fetch = "SELECT globus_id FROM globus_tasks WHERE status = '%s'" % status
        results = []
    else:
        q_fetch = "SELECT globus_id, status, started, completed, globus_user, " \
                  "file_count, bytes, contents FROM globus_tasks WHERE status = '%s'" % status
        results = {}


    curs = psql_conn.cursor()
    #logger.debug("Fetching all %s tasks from PostgreSQL..." % status)
    curs.execute(q_fetch)
    for result in curs:
        if id_only:
            # Just add globus ID to list
            results.append(result[0])
        else:
            # Add record to dictionary, with globus ID as key
            gid = result[0]
            results[gid] = {
                "globus_id": gid,
                "status": result[1],
                "started": result[2],
                "completed": result[3],
                "user": result[4],
                "file_count": result[5],
                "bytes": result[6],
                "contents": result[7]
            }
    curs.close()

    return results

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
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Use globus goauth tool to get access token for config account"""
def generateAuthToken():
    logger.info("- generating auth token for "+config['globus']['username'])
    t = goauth.get_access_token(
            username=config['globus']['username'],
            password=config['globus']['password']
    ).token
    config['globus']['auth_token'] = t
    logger.debug("- generated: "+t)

"""Refresh auth token and send autoactivate message to source and destination Globus endpoints"""
def activateEndpoints():
    src = config['globus']["source_endpoint_id"]
    dest = config['globus']["destination_endpoint_id"]

    generateAuthToken()
    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
    # TODO: Can't use autoactivate; must populate credentials
    """try:
        actList = api.endpoint_activation_requirements(src)[2]
        actList.set_requirement_value('myproxy', 'username', 'data_mover')
        actList.set_requirement_value('myproxy', 'passphrase', 'terraref2016')
        actList.set_requirement_value('delegate_proxy', 'proxy_chain', 'some PEM cert w public key')
    except:"""
    api.endpoint_autoactivate(src)
    api.endpoint_autoactivate(dest)

"""Send message to NCSA Globus monitor API that a new task has begun"""
def notifyMonitorOfNewTransfer(globusID, contents, sess):
    logger.info("%s being sent to NCSA Globus monitor" % globusID, extra={
        "globus_id": globusID,
        "action": "NOTIFY NCSA MONITOR"
    })

    try:
        status = sess.post(config['ncsa_api']['host']+"/tasks", data=json.dumps({
            "user": config['globus']['username'],
            "globus_id": globusID,
            "contents": contents
        }))
        return status

    except requests.ConnectionError as e:
        logger.error("- cannot connect to NCSA API")
        return {'status_code':503}

"""Contact NCSA Globus monitor API to check whether task was completed successfully"""
def getTransferStatusFromMonitor(globusID):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

    # Check with Globus monitor rather than Globus itself, to make sure file was handled properly before deleting from src
    try:
        st = sess.get(config['ncsa_api']['host']+"/tasks/"+globusID)
        if st.status_code == 200:
            return json.loads(st.text)['status']
        elif st.status_code == 404:
            return "NOT FOUND"
        else:
            logger.error("%s monitor status check failed (%s: %s)" % (globusID, st.status_code, st.text), extra={
                "globus_id": globusID
            })
            return "UNKNOWN"
    except requests.ConnectionError as e:
        logger.error("- cannot connect to NCSA API")
        return None

"""Continually initiate transfers from pending queue and contact NCSA API for status updates"""
def globusMonitorLoop():
    global activeTasks

    # Prepare timers for tracking how often different refreshes are executed
    apiWait = config['ncsa_api']['api_check_frequency_secs'] # check status of sent files
    authWait = config['globus']['authentication_refresh_frequency_secs'] # renew globus auth

    while True:
        time.sleep(1)
        apiWait -= 1
        authWait -= 1

        if apiWait <= 0:
            # First, try to notify NCSA about tasks it isn't aware of
            logger.debug("- attempting to notify NCSA of Globus tasks")
            sess = requests.Session()
            sess.auth = (config['globus']['username'], config['globus']['password'])

            # CREATED -> IN PROGRESS after NCSA notification
            current_tasks = readTasksByStatus("CREATED")
            for task in current_tasks:
                notify = notifyMonitorOfNewTransfer(task['globus_id'], task['contents'], sess)
                if notify.status_code == 200:
                    task['status'] = "IN PROGRESS"
                    writeTaskToDatabase(task)

            # SUCCEEDED -> NOTIFIED after NCSA notification
            current_tasks = readTasksByStatus("SUCCEEDED")
            for task in current_tasks:
                notify = notifyMonitorOfNewTransfer(task['globus_id'], task['contents'], sess)
                if notify.status_code == 200:
                    task['status'] = "NOTIFIED"
                    writeTaskToDatabase(task)

            # Next, check In Progress xfers with Globus to mark as completed
            currentlyActive = readTasksByStatus("CREATED")
            for task in currentlyActive:
                # ON COMPLETE, UPDATE TO SUCCEEDED
                checkWithGlobus(task)
            currentlyActive = readTasksByStatus("IN PROGRESS")
            for task in currentlyActive:
                # ON COMPLETE, UPDATE TO NOTIFIED
                checkWithGlobus(task)

            # Finally, check with NCSA about whether it has processed others
            logger.debug("- GLOBUS THREAD: checking status of %s active tasks with NCSA Globus monitor" % len(activeTasks))
            currentlyActive = readTasksByStatus("NOTIFIED")
                # ON COMPLETE, UPDATE TO PROCESSED


            for globusID in currentActiveTasks:
                task = activeTasks[globusID]

                globusStatus = getTransferStatusFromMonitor(globusID)
                if globusStatus in ["SUCCEEDED", "PROCESSED", "FAILED"]:
                    logger.info("%s status update received: %s" % (globusID, globusStatus), extra={
                        "globus_id": globusID,
                        "action": "STATUS UPDATE",
                        "status": globusStatus
                    })
                    task['status'] = globusStatus
                    task['completed'] = str(datetime.datetime.now())

                    # Write out results log
                    writeCompletedTransferToDisk(task)

                # If the Globus monitor isn't even aware of this transfer, try to notify it!
                elif globusStatus == "NOT FOUND":
                    notifyMonitorOfNewTransfer(globusID, task['contents'])

                else:
                    # Couldn't connect to NCSA API, wait for next loop to try again
                    break

            apiWait = config['ncsa_api']['api_check_frequency_secs']
            writeTasksToDisk(os.path.join(config['log_path'], "active_tasks.json"), activeTasks)

        # Refresh Globus auth tokens
        if authWait <= 0:
        #    generateAuthToken()
            authWait = config['globus']['authentication_refresh_frequency_secs']

"""Continually contact NCSA API for status updates"""
def globusCleanupLoop():
    global activeTasks

    # Prepare timers for tracking how often different refreshes are executed
    cleanWait = config['ncsa_api']['api_check_frequency_secs'] # check status of sent files

    while True:
        time.sleep(1)
        cleanWait -= 1

        # Check with NCSA Globus monitor API for completed transfers
        if cleanWait <= 0:
            # TODO: Can symlinks go away entirely now?
            logger.debug("- CLEANUP THREAD: creating symlinks for completed tasks")
            cleanedCount = 0
            # Use copy of task list so it doesn't change during iteration
            currentlyActiveTasks = safeCopy(activeTasks)
            for gid in currentlyActiveTasks:
                task = activeTasks[gid]

                if task['status'] in ["SUCCEEDED", "PROCESSED", "FAILED"]:
                    # Move files to staging area for deletion
                    if task['status'] == "SUCCEEDED" or task['status'] == "PROCESSED":
                        logger.info("%s creating symlinks" % gid)
                        deleteDir = config['gantry']['deletion_queue']
                        if deleteDir != "":
                            for ds in task['contents']:
                                if 'files' in task['contents'][ds]:
                                    for f in task['contents'][ds]['files']:
                                        fobj = task['contents'][ds]['files'][f]
                                        if 'orig_path' in fobj:
                                            srcPath = os.path.join(config['globus']['source_path'], fobj['orig_path'])
                                        else:
                                            # TODO: This can go away once transitional xfers are handled
                                            cp = fobj['path']
                                            if cp.find("/EnvironmentLogger")>-1 or cp.find("/3DScannerRawDataTopTmp")>-1:
                                                cp = cp.replace("/ua-mac/raw_data", "/LemnaTec")
                                            elif cp.find("/lightning")>-1 or cp.find("/weather")>-1 or cp.find("/irrigation")>-1:
                                                cp = cp.replace("/ua-mac/raw_data", "/MAC")
                                            else: # MovingSensors
                                                cp = cp.replace("/ua-mac/raw_data", "/LemnaTec/MovingSensors")
                                            srcPath = os.path.join(config['globus']['source_path'], cp)
                                        if fobj['name'] not in srcPath:
                                            srcPath = os.path.join(srcPath, fobj['name'])
                                        createLocalSymlink(srcPath, srcPath.replace(config['globus']['source_path'], deleteDir))

                                        # Crawl and remove empty directories
                                        # logger.info("- removing empty directories in "+config['gantry']['incoming_files_path'])
                                        # subprocess.call(["find", config['gantry']['incoming_files_path'], "-type", "d", "-empty", "-delete"])

                    cleanedCount += 1
                    if gid in activeTasks:
                        del activeTasks[gid]
                        writeTasksToDisk(os.path.join(config['log_path'], "active_tasks.json"), activeTasks)

            logger.debug("- CLEANUP THREAD: cleaned %s tasks" % cleanedCount)

            # Reset timer to check NCSA api for transfer updates again
            cleanWait = config['ncsa_api']['api_check_frequency_secs']

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
        main_log_file = os.path.join(config["log_path"], "log_monitor.txt")
        log_config['handlers']['file']['filename'] = main_log_file
        if not os.path.exists(config["log_path"]):
            os.makedirs(config["log_path"])
        if not os.path.isfile(main_log_file):
            open(main_log_file, 'a').close()
        logging.config.dictConfig(log_config)
    logger = logging.getLogger('gantry')

    # Load any previous active/pending transfers
    psql_conn = connectToPostgres()
    activateEndpoints()

    logger.info("*** Service now monitoring existing Globus transfers ***")
    thread.start_new_thread(globusMonitorLoop, ())

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

# Used by the FTP log reader to track progress
status_lastFTPLogLine = ""
status_lastNasLogLine = ""
status_numPending = 0
status_numActive = 0

"""pendingTransfers tracks files prepped for transfer, by dataset:
{
    "dataset": {
        "files": {
            "filename1": {
                "name": "filename1",
                "md": {},
                "md_name": "name_of_metadata_file"
                "md_path": "folder_containing_metadata_file"},
            "filename2": {...},
            ...
        },
        "md": {},
        "md_path": "folder_containing_metadata.json"
    },
    "dataset2": {...},
...}"""
pendingTransfers = {}

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

"""Return small JSON object with information about monitor health"""
def getStatus():
    # TODO: Find a more efficient solution here, or only update periodically
    #completedTasks = 0
    #for root, dirs, filelist in os.walk(config['completed_tasks_path']):
    #    completedTasks += len(filelist)

    return {
        "pending_file_transfers": status_numPending,
        "active_globus_tasks": status_numActive,
        #"completed_globus_tasks": completedTasks,
        "last_ftp_log_line_read": status_lastFTPLogLine,
        "last_nas_log_line_read": status_lastNasLogLine
    }

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

"""Clear out any datasets from pendingTransfers without files or metadata"""
def cleanPendingTransfers():
    global pendingTransfers
    global status_numPending

    logger.debug("- tidying up pending transfer queue")

    status_numPending = 0
    allPendingTransfers = safeCopy(pendingTransfers)

    for ds in allPendingTransfers:
        dsobj = allPendingTransfers[ds]
        if 'files' in dsobj:
            status_numPending += len(dsobj['files'])
            if len(dsobj['files']) == 0:
                del pendingTransfers[ds]['files']

        if 'md' in dsobj and len(dsobj['md']) == 0:
            del pendingTransfers[ds]['md']
            if 'md_path' in dsobj:
                del pendingTransfers[ds]['md_path']

        if 'files' not in pendingTransfers[ds] and 'md' not in pendingTransfers[ds]:
            del pendingTransfers[ds]

"""Make entry for pendingTransfers"""
def prepFileForPendingTransfers(f, sensorname=None, timestamp=None, datasetname=None, filemetadata={},
                              manual=False):
    global pendingTransfers
    global status_numPending

    # Strip portions of paths that differ between <true source path> and <internal Docker path>
    gantryDir = config['gantry']['incoming_files_path']
    dockerDir = config['globus']['source_path']
    whitelist = config['gantry']["directory_whitelist"]

    # Only check whitelist if scanning from FTP, not if manually submitted
    if not manual:
        whitelisted = False
        for w in whitelist:
            if f.find(w) == 0 or f.find(w.replace(dockerDir,"")) == 0:
                whitelisted = True
        if not whitelisted:
            logger.error("path %s is not whitelisted; skipping" % f)
            return

    # Get path starting at the site level (e.g. LemnaTec, MAC)
    if f.find(gantryDir) > -1:
        # try with and without leading /
        gantryDirPath = f.replace(gantryDir, "")
        gantryDirPath = gantryDirPath.replace(gantryDir[1:], "")
    else:
        # try with and without leading /
        gantryDirPath = f.replace(dockerDir, "")
        gantryDirPath = gantryDirPath.replace(dockerDir[1:], "")

    # Get meta info from file path
    # /LemnaTec/EnvironmentLogger/2016-01-01/2016-08-03_04-05-34_environmentlogger.json
    # /LemnaTec/MovingSensor/co2Sensor/2016-01-01/2016-08-02__09-42-51-195/file.json
    # /MAC/lightning/2016-01-01/weather_2016_06_29.dat
    # /LemnaTec/MovingSensor.reproc2016-8-18/scanner3DTop/2016-08-22/2016-08-22__15-13-01-672/6af8d63b-b5bb-49b2-8e0e-c26e719f5d72__Top-heading-east_0.png
    # /LemnaTec/MovingSensor.reproc2016-8-18/scanner3DTop/2016-08-22/2016-08-22__15-13-01-672/6af8d63b-b5bb-49b2-8e0e-c26e719f5d72__Top-heading-east_0.ply
    # /Users/mburnette/globus/sorghum_pilot_dataset/snapshot123456/file.png
    pathParts = gantryDirPath.split("/")

    # Filename is always last
    filename = pathParts[-1]

    # Timestamp is one level up from filename
    if not timestamp:
        timestamp = pathParts[-2]  if len(pathParts)>1 else "unknown_time"

    # Sensor name varies based on folder structure
    if not sensorname:
        if timestamp.find("__") > -1 and len(pathParts) > 3:
            sensorname = pathParts[-4]
        elif len(pathParts) > 2:
            sensorname = pathParts[-3].replace("EnviromentLogger", "EnvironmentLogger")
        else:
            sensorname = "unknown_sensor"

    if not datasetname:
        datasetname = sensorname +" - "+timestamp

    gantryDirPath = gantryDirPath.replace(filename, "")

    newTransfer = {
        datasetname: {
            "files": {
                filename: {
                    "name": filename,
                    "path": gantryDirPath[1:] if gantryDirPath[0]=="/" else gantryDirPath,
                    "orig_path": f
                }
            }
        }
    }
    if filemetadata and filemetadata != {}:
        newTransfer[datasetname]["files"][filename]["md"] = filemetadata

    return newTransfer
    pendingTransfers = updateNestedDict(safeCopy(pendingTransfers), newTransfer)

"""Take line of FTP transfer log and parse a datetime object from it"""
def parseDateFromFTPLogLine(line):
    # Example log line:
    #Tue Apr  5 12:35:58 2016 1 ::ffff:150.135.84.81 4061858 /gantry_data/LemnaTec/EnvironmentLogger/2016-04-05/2016-04-05_12-34-58_enviromentlogger.json b _ i r lemnatec ftp 0 * c

    months = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }

    l = line.split()
    if len(l) > 6:
        YY = int(l[4])      # year, e.g. 2016
        MM = months[l[1]]   # month, e.g. 4
        DD = int(l[2])      # day of month, e.g. 5
        hh = int(l[3].split(':')[0])  # hours, e.g. 12
        mm = int(l[3].split(':')[1])  # minutes, e.g. 35
        ss = int(l[3].split(':')[2])  # seconds, e.g. 58

        return datetime.datetime(YY, MM, DD, hh, mm, ss)
    else:
        # TODO: How to handle unparseable log line?
        return datetime.datetime(1900, 1, 1, 1, 1, 1)

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
# API COMPONENTS
# ----------------------------------------------------------
""" /files
Add a file to the transfer queue manually so it can be sent to NCSA Globus"""
class TransferQueue(restful.Resource):

    def post(self):
        """
        Example POST content:
            {
                "path": "file1.txt",
                "md": {...metadata object...},
                "dataset_name": "snapshot123456",
                "space_id": "571fbfefe4b032ce83d96006"
            }
        ...or...
            {
                "paths": ["file1.txt", "file2.jpg", "file3.jpg"...],
                "file_metadata": {
                    "file1.txt": {...metadata object...},
                    "file2.jpg": {...metadata object...}
                }
                "dataset_name": "snapshot123456",
                "space_id": "571fbfefe4b032ce83d96006"
            }
        ...or...
            {
                "paths": ["file1.txt", "file2.jpg", "file3.jpg"...],
                "sensor_name": "VIS",
                "timestamp": "2016-06-29__10-28-43-323",
                "space_id": "571fbfefe4b032ce83d96006"
            }

        In the second example, resulting dataset is called "VIS - 2016-06-29__10-28-43-323"

        To associate metadata with the given dataset, include a "metadata.json" file.
        """
        global pendingTransfers

        req = request.get_json(force=True)
        srcpath = config['globus']['source_path']

        sensorname = req['sensor_name'] if 'sensor_name' in req else None
        datasetname = req['dataset_name'] if 'dataset_name' in req else None
        timestamp = req['timestamp'] if 'timestamp' in req else None
        spaceid = req['space_id'] if 'space_id' in req else None

        # Single file path entry under 'path'
        if 'path' in req:
            p = req['path']
            if p.find(srcpath) == -1:
                p = os.path.join(srcpath, p)
            filemetadata = {} if 'md' not in req else req['md']

            newTransfer = prepFileForPendingTransfers(p, sensorname, timestamp, datasetname, filemetadata, True)
            if spaceid:
                newTransfer['space_id'] = spaceid
            pendingTransfers = updateNestedDict(safeCopy(pendingTransfers), newTransfer)
            logger.info("- file queued via API: %s" % p)

        # Multiple file path entries under 'paths'
        if 'paths' in req:
            allNewTransfers = {}
            if spaceid:
                allNewTransfers['space_id'] = spaceid
            for p in req['paths']:
                if p.find(srcpath) == -1:
                    p = os.path.join(srcpath, p)
                if 'file_metadata' in req:
                    if p in req['file_metadata']:
                        filemetadata = req['file_metadata'][p]
                    elif os.path.basename(p) in req['file_metadata']:
                        filemetadata = req['file_metadata'][os.path.basename(p)]
                    else:
                        filemetadata = {}
                else:
                    filemetadata = {}

                newTransfer = prepFileForPendingTransfers(p, sensorname, timestamp, datasetname, filemetadata, True)
                if spaceid:
                    newTransfer['space_id'] = spaceid
                allNewTransfers = updateNestedDict(allNewTransfers, newTransfer)
                logger.info("- file queued via API: %s" % p)

            pendingTransfers = updateNestedDict(safeCopy(pendingTransfers), allNewTransfers)

        cleanPendingTransfers()
        writeTasksToDisk(os.path.join(config['log_path'], "pending_transfers.json"), pendingTransfers)
        return 201

""" / status
Return basic information about monitor for health checking"""
class MonitorStatus(restful.Resource):

    def get(self):
        return getStatus(), 200

api.add_resource(TransferQueue, '/files')
api.add_resource(MonitorStatus, '/status')

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

"""Generate a submission ID that can be used to avoid double-submitting"""
def generateGlobusSubmissionID():
    try:
        api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
        status_code, status_message, submission_id = api.submission_id()
    except:
        try:
            # Try activating endpoints and retrying
            activateEndpoints()
            api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
            status_code, status_message, submission_id = api.submission_id()
        except:
            logger.error("- exception generating submission ID for Globus transfer")
            return None

    if status_code == 200:
        logger.debug("- generated new Globus submission ID %s" % submission_id['value'])
        return submission_id['value']
    else:
        logger.error("- could not generate new Globus submission ID (%s: %s)" % (status_code, status_message))
        return None

"""Check for files ready for transmission and queue them"""
def queueGantryFilesForTransfer():
    global pendingTransfers

    foundFiles = []
    maxPending = config["gantry"]["max_pending_files"]

    # Get list of files from FTP log, if log is specified
    if status_numPending < maxPending:
        foundFiles = getNewFilesFromFTPLogs()

    # Get list of files from watched folders, if folders are specified  (and de-duplicate from FTP list)
    if status_numPending+len(foundFiles) < maxPending:
        fileList = getNewFilesFromWatchedFolders(len(foundFiles))
        for found in fileList:
            if found not in foundFiles:
                foundFiles.append(found)

    logger.debug("- adding %s found files into pending queue" % len(foundFiles))
    try:
        newXfers = {}
        for f in foundFiles:
            # Skip hidden/system files
            if f == "" or f.split("/")[-1][0] == ".":
                continue
            newTransfer = prepFileForPendingTransfers(f)
            newXfers = updateNestedDict(newXfers, newTransfer)
        pendingTransfers = updateNestedDict(safeCopy(pendingTransfers), newXfers)
    except Exception as e:
        logger.error("problem adding files: %s" % str(e))
    logger.debug("- additions to pending queue complete")

"""Check folders in config for files older than the configured age, and queue for transfer"""
def getNewFilesFromWatchedFolders(alreadyFound):
    foundFiles = []

    # Get list of files last modified more than X minutes ago
    watchDirs = config['gantry']['file_age_monitor_paths']
    fileAge = config['gantry']['min_file_age_for_transfer_mins']
    maxPending = config["gantry"]["max_pending_files"]

    for currDir in watchDirs:
        # TODO: Check for hidden files beginning with "." or just allow them?
        foundList = subprocess.check_output(["find", currDir, "-mmin", "+"+fileAge, "-type", "f", "-print"]).split("\n")
        if len(foundList)+len(foundFiles)+alreadyFound+status_numPending > maxPending:
            break
        else:
            for f in foundList:
                foundFiles.append(f)

    logger.info("- found %s files from watched folders" % len(foundFiles))
    return foundFiles

"""Check FTP log files to determine new files that were successfully moved to staging area"""
def getNewFilesFromFTPLogs():
    global status_lastFTPLogLine, status_lastNasLogLine

    current_lastFTPLogLine = status_lastFTPLogLine
    current_lastNasLogLine = status_lastNasLogLine
    maxPending = config["gantry"]["max_pending_files"]
    logDir = config["gantry"]["ftp_log_path"]
    foundFiles = []
    if logDir == "":
        # Don't perform a scan if no log file is defined
        return foundFiles

    # NASLOG - handle first as it will have fewer files/less frequent updates -----------------------------
    # /gantry_data/MAC/lightning/2016-06-29/LW110_ALARM1min.dat
    logger.info("- reading naslog from: "+status_lastNasLogLine)
    lastLine = copy.copy(status_lastNasLogLine)
    currLog = os.path.join(logDir, "naslog")
    foundResumePoint = True # TODO: False to re-enable
    while not foundResumePoint:
        nasfound = 0
        with open(currLog, 'r+') as f:
            if lastLine == "":
                foundResumePoint = True

            for line in f:
                line = line.replace("\n","").rstrip()

                if line == lastLine:
                    logger.debug("- found the resume point")
                    foundResumePoint = True

                elif foundResumePoint:
                    if line != "":
                        current_lastNasLogLine = line

                    # Check if file still exists before queuing for Globus
                    if os.path.exists(line.replace(config['globus']['source_path'], config['gantry']['incoming_files_path'])):
                        line = line.replace(config['globus']['source_path'],"")
                        foundFiles.append(line)
                        nasfound += 1
                    else:
                        logger.info("Skipping missing file from naslog: "+line)

                if status_numPending+len(foundFiles) >= maxPending:
                    break
        if nasfound > 0:
            logger.info(" - found %s lines from naslog" % nasfound)

    # XFERLOG -  archived files are by date, e.g. "xferlog-20160501", "xferlog-20160502" --------------------------
    # Tue Apr  5 12:35:58 2016 1 ::ffff:150.135.84.81 4061858 /gantry_data/LemnaTec/EnvironmentLogger/2016-04-05/2016-04-05_12-34-58_enviromentlogger.json b _ i r lemnatec ftp 0 * c
    if status_numPending+len(foundFiles) < maxPending:
        logger.info("- reading xfer logs from: "+status_lastFTPLogLine)
        def isOldXferLog(fname):
            return fname.find("xferlog-") > -1
        lognames = filter(isOldXferLog, os.listdir(logDir))
        lognames.sort()

        lastLine = copy.copy(status_lastFTPLogLine)
        lastReadTime = parseDateFromFTPLogLine(lastLine)

        currLog = os.path.join(logDir, "xferlog")
        backLog = 0
        foundResumePoint = False
        handledBackLog = True
        while not (foundResumePoint and handledBackLog):
            with (open(currLog, 'r+') if currLog.find(".gz")==-1 else gzip.open(currLog, 'r+')) as f:
                logger.info("- scanning "+currLog)
                # If no most recent scanned line available, just start from beginning of current log file
                if lastLine == "":
                    initialLine = False
                    foundResumePoint = True
                else:
                    initialLine = True

                for line in f:
                    line = line.rstrip()
                    if initialLine and handledBackLog:
                        firstLineTime = parseDateFromFTPLogLine(line)
                        if firstLineTime <= lastReadTime:
                            # File begins before most recent line - should be scanned
                            initialLine = False
                        elif firstLineTime > lastReadTime:
                            # File begins after most recent line - need to go back 1 file at least
                            break

                    if line == lastLine:
                        logger.debug("- found the resume point")
                        foundResumePoint = True

                    elif foundResumePoint:
                        if line != "":
                            current_lastFTPLogLine = line

                        # We're past the last scanned line, so capture these lines if complete & ending in 'c'
                        if re.search('ftp \d \* c', line.rstrip()):
                            # Extract filename from log entry, after an IP address and a number (byte count?)
                            fnameRegex = '::ffff:\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3} \d+ ((\/?.)+) +\w _'
                            fname = re.search(fnameRegex, line)
                            if fname:
                                fullname = fname.group(1).rstrip()
                                # Check if file still exists before queuing for Globus
                                if os.path.exists(fullname.replace(config['globus']['source_path'], config['gantry']['incoming_files_path'])):
                                    fullname = fullname.replace(config['globus']['source_path'],"")
                                    foundFiles.append(fullname)
                                else:
                                    logger.info("Skipping missing file from FTP log: "+fullname)

                    if status_numPending+len(foundFiles) >= maxPending:
                        break

            # If we didn't find last line in this file, look into the previous file
            if not foundResumePoint:
                handledBackLog = False
                backLog += 1

                if abs(backLog) > len(lognames):
                    # No previous logs, so just start with current one
                    currLog = os.path.join(logDir, "xferlog")
                    foundResumePoint = True
                    backLog = 0

                    logger.error("LAST READ LINE NOT FOUND! Will search again next cycle.")
                    handledBackLog = True
                else:
                    currLog = os.path.join(logDir, lognames[-backLog])

                logger.debug("- walking back to %s" % currLog)

            # If we filled up the pending queue handling backlog, don't move onto newer files yet
            elif len(foundFiles)+status_numPending >= maxPending:
                logger.debug("- maximum number of pending files reached")
                handledBackLog = True

            # If we found last line in a previous file, climb back up to current file and get its contents too
            elif backLog > 0:
                backLog -= 1
                if backLog != 0:
                    currLogName = lognames[-backLog]
                    #currLogName = "xferlog-"+str(backLog) if backLog > 0 else "xferlog"
                else:
                    currLogName = "xferlog"
                currLog = os.path.join(logDir, currLogName)
                logger.debug("- walking up to %s" % currLog)

            # If we found the line and handled all backlogged files, we're ready to go
            else:
                handledBackLog = True

    status_lastFTPLogLine = current_lastFTPLogLine
    status_lastNasLogLine = current_lastNasLogLine
    logger.debug("- found %s files from logs" % len(foundFiles))
    return foundFiles

"""Initiate Globus transfer with batch of files and add to activeTasks - recurse until max xfers reached or pending empty """
def initializeGlobusTransfer():
    global pendingTransfers
    global activeTasks
    global status_numActive
    global status_numPending

    maxQueue = config['globus']['max_transfer_file_count']

    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
    submissionID = generateGlobusSubmissionID()

    if submissionID:
        # Prepare transfer object
        transferObj = Transfer(submissionID,
                               config['globus']['source_endpoint_id'],
                               config['globus']['destination_endpoint_id'],
                               verify_checksum=True)

        queueLength = 0

        # Loop over a copy of the list instead of actual thing - other thread will be appending to actual thing
        loopingTransfers = safeCopy(pendingTransfers)
        currentTransferBatch = {}

        logger.debug("- building transfer %s from pending queue" % submissionID)
        for ds in loopingTransfers:
            if "files" in loopingTransfers[ds]:
                if ds not in currentTransferBatch:
                    currentTransferBatch[ds] = {}
                    currentTransferBatch[ds]['files'] = {}
                # Add files from each dataset
                for f in loopingTransfers[ds]['files']:
                    if queueLength < maxQueue:
                        fobj = loopingTransfers[ds]['files'][f]
                        if fobj["path"].find(config['globus']['source_path']) > -1:
                            src_path = os.path.join(fobj["path"], fobj["name"])
                            dest_path = os.path.join(config['globus']['destination_path'],
                                                     fobj["path"].replace(config['globus']['source_path'], ""),
                                                     fobj["name"])
                        else:
                            src_path = os.path.join(config['globus']['source_path'], fobj["path"], fobj["name"])
                            dest_path = os.path.join(config['globus']['destination_path'], fobj["path"],  fobj["name"])

                        # Clean up dest path to new folder structure
                        # ua-mac/raw_data/LemnaTec/EnvironmentLogger/2016-01-01/2016-08-03_04-05-34_environmentlogger.json
                        # ua-mac/raw_data/LemnaTec/MovingSensor/co2Sensor/2016-01-01/2016-08-02__09-42-51-195/file.json
                        # ua-mac/raw_data/LemnaTec/3DScannerRawDataTopTmp/scanner3DTop/2016-01-01/2016-08-02__09-42-51-195/file.json
                        # ua-mac/raw_data/MAC/lightning/2016-01-01/weather_2016_06_29.dat
                        dest_path = dest_path.replace("LemnaTec/", "")
                        dest_path = dest_path.replace("MovingSensor/", "")
                        dest_path = dest_path.replace("MAC/", "")
                        dest_path = dest_path.replace("3DScannerRawDataTopTmp/", "")
                        dest_path = dest_path.replace("3DScannerRawDataLowerOnEastSideTmp/", "")
                        dest_path = dest_path.replace("3DScannerRawDataLowerOnWestSideTmp/", "")
                        # /ua-mac/raw_data/MovingSensor.reproc2016-8-18/scanner3DTop/2016-08-22/2016-08-22__15-13-01-672/6af8d63b-b5bb-49b2-8e0e-c26e719f5d72__Top-heading-east_0.png
                        # /ua-mac/raw_data/MovingSensor.reproc2016-8-18/scanner3DTop/2016-08-22/2016-08-22__15-13-01-672/6af8d63b-b5bb-49b2-8e0e-c26e719f5d72__Top-heading-east_0.ply
                        if dest_path.endswith(".ply") and (dest_path.find("scanner3DTop") or
                                                               dest_path.find("scanner3DLowerOnEastSide") or
                                                               dest_path.find("scanner3DLowerOnWestSide")) > -1:
                            dest_path = dest_path.replace("raw_data", "Level_1")
                        if dest_path.find("MovingSensor.reproc") > -1:
                            new_dest_path = ""
                            dirs = dest_path.split("/")
                            for dir_part in dirs:
                                if dir_part.find("MovingSensor.reproc") == -1:
                                    new_dest_path = os.path.join(new_dest_path, dir_part)
                            dest_path = new_dest_path
                        # danforth/raw_data/sorghum_pilot_dataset/snapshot123456/file.png

                        transferObj.add_item(src_path, dest_path)

                        # remainingTransfers will have leftover data once max Globus transfer size is met
                        queueLength += 1
                        fobj["orig_path"] = fobj["path"]
                        fobj["path"] = dest_path
                        currentTransferBatch[ds]['files'][f] = fobj
                    else:
                        break

                # Clean up placeholder entries once queue length is exceeded
                if currentTransferBatch[ds]['files'] == {}:
                    del currentTransferBatch[ds]['files']
                if currentTransferBatch[ds] == {}:
                    del currentTransferBatch[ds]

            if queueLength >= maxQueue:
                break

        if queueLength > 0:
            # Send transfer to Globus
            try:
                logger.debug("- attempting to send new transfer")
                status_code, status_message, transfer_data = api.transfer(transferObj)
            except (APIError, ClientError) as e:
                try:
                    # Try refreshing endpoints and retrying
                    activateEndpoints()
                    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
                    status_code, status_message, transfer_data = api.transfer(transferObj)
                except (APIError, ClientError) as e:
                    logger.error("- problem initializing Globus transfer")
                    status_code = 503
                    status_message = e
                except:
                    logger.error("- unexpected problem initializing Globus transfer")
                    status_code = 503
                    status_message = e

            if status_code == 200 or status_code == 202:
                # Notify NCSA monitor of new task, and add to activeTasks for logging
                globusID = transfer_data['task_id']
                logger.info("%s new Globus transfer task started (%s files)" % (globusID, queueLength), extra={
                    "globus_id": globusID,
                    "action": "TRANSFER STARTED",
                    "contents": currentTransferBatch
                })

                activeTasks[globusID] = {
                    "globus_id": globusID,
                    "contents": currentTransferBatch,
                    "started": str(datetime.datetime.now()),
                    "status": "IN PROGRESS"
                }
                writeTasksToDisk(os.path.join(config['log_path'], "active_tasks.json"), activeTasks)

                # Now that we've safely sent the pending transfers, remove them
                for ds in currentTransferBatch:
                    if ds in pendingTransfers:
                        for f in currentTransferBatch[ds]['files']:
                            if f in pendingTransfers[ds]['files']:
                                del pendingTransfers[ds]['files'][f]

                notifyMonitorOfNewTransfer(globusID, currentTransferBatch)
                writeTasksToDisk(os.path.join(config['log_path'], "pending_transfers.json"), pendingTransfers)
            else:
                # If failed, leave pending list as-is and try again on next iteration (e.g. in 180 seconds)
                logger.error("- Globus transfer initialization failed for %s (%s: %s)" % (ds, status_code, status_message))
                return

        status_numActive = len(activeTasks)
        cleanPendingTransfers()

"""Send message to NCSA Globus monitor API that a new task has begun"""
def notifyMonitorOfNewTransfer(globusID, contents):
    sess = requests.Session()
    sess.auth = (config['globus']['username'], config['globus']['password'])

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
    global status_numActive

    # Prepare timers for tracking how often different refreshes are executed
    globusWait = config['gantry']['globus_transfer_frequency_secs'] # bundle pending files and transfer
    apiWait = config['ncsa_api']['api_check_frequency_secs'] # check status of sent files
    authWait = config['globus']['authentication_refresh_frequency_secs'] # renew globus auth

    while True:
        time.sleep(1)
        globusWait -= 1
        apiWait -= 1
        authWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if globusWait <= 0:
            logger.debug("- GLOBUS THREAD: %s/%s active tasks" % (status_numActive, config["globus"]["max_active_tasks"]))

            if status_numActive < config["globus"]["max_active_tasks"]:
                logger.debug("- checking pending file list...")
                # Clean up the pending object of straggling keys, then initialize Globus transfers
                cleanPendingTransfers()
                while status_numPending > 0 and status_numActive < config['globus']['max_active_tasks']:
                    logger.info("- pending files found. initializing Globus transfer.")
                    writeTasksToDisk(os.path.join(config['log_path'], "pending_transfers.json"), pendingTransfers)
                    initializeGlobusTransfer()

            # Reset wait to check gantry incoming directory again
            globusWait = config['gantry']['globus_transfer_frequency_secs']
            writeTasksToDisk(os.path.join(config["log_path"], "monitor_status.json"), getStatus())

        # Check with NCSA Globus monitor API for completed transfers
        if apiWait <= 0:
            # First, try to notify NCSA about tasks it isn't aware of
            logger.debug("- attempting to notify NCSA of created Globus tasks")
            currentlyActive = readTasksByStatus("CREATED")
            for task in currentlyActive:
                # ON SUCCESS, UPDATE TO IN PROGRESS
                notifyMonitorOfNewTransfer(task['globus_id'], task['contents'])
            currentlyActive = readTasksByStatus("SUCCEEDED")
            for task in currentlyActive:
                # ON SUCCESS, UPDATE TO NOTIFIED
                notifyMonitorOfNewTransfer(task['globus_id'], task['contents'])

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
    global status_numActive

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

            status_numActive = len(activeTasks)
            logger.debug("- CLEANUP THREAD: cleaned %s tasks" % cleanedCount)

            # Reset timer to check NCSA api for transfer updates again
            cleanWait = config['ncsa_api']['api_check_frequency_secs']
            writeTasksToDisk(os.path.join(config["log_path"], "monitor_status.json"), getStatus())

"""Continually monitor FTP log for new files to transmit and add them to pendingTransfers"""
def gantryMonitorLoop():
    gantryWait = 1 # look for new files to send

    while True:
        time.sleep(1)
        gantryWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if gantryWait <= 0:
            logger.debug("LOG SCAN THREAD: %s/%s files pending" % (status_numPending, config["gantry"]["max_pending_files"]))
            if status_numPending < config["gantry"]["max_pending_files"]:
                queueGantryFilesForTransfer()
                cleanPendingTransfers()
                logger.debug("LOG SCAN THREAD: now %s pending transfers" % status_numPending)
                if status_numPending > 0:
                    writeTasksToDisk(os.path.join(config['log_path'], "pending_transfers.json"), pendingTransfers)
                writeTasksToDisk(os.path.join(config["log_path"], "monitor_status.json"), getStatus())

            # Reset wait to check gantry incoming directory again
            gantryWait = config['gantry']['file_check_frequency_secs']

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
        main_log_file = os.path.join(config["log_path"], "log.txt")
        log_config['handlers']['file']['filename'] = main_log_file
        if not os.path.exists(config["log_path"]):
            os.makedirs(config["log_path"])
        if not os.path.isfile(main_log_file):
            open(main_log_file, 'a').close()
        logging.config.dictConfig(log_config)
    logger = logging.getLogger('gantry')

    # Get last read log line from previous run
    if os.path.exists(os.path.join(config["log_path"], "monitor_status.json")):
        monitorData = loadJsonFile(os.path.join(config["log_path"], "monitor_status.json"))
        status_lastFTPLogLine = monitorData["last_ftp_log_line_read"]
        if "last_nas_log_line_read" in monitorData:
            status_lastNasLogLine = monitorData["last_nas_log_line_read"]
        else:
            status_lastNasLogLine = ""

    # Load any previous active/pending transfers
    psql_conn = connectToPostgres()

    activeTasks = loadTasksFromDisk(os.path.join(config['log_path'], "active_tasks.json"))

    status_numActive = len(activeTasks)
    pendingTransfers = loadTasksFromDisk(os.path.join(config['log_path'], "pending_transfers.json"))
    cleanPendingTransfers()

    logger.info("- loaded data from active and pending log files")
    logger.info("- %s pending files" % status_numPending)
    logger.info("- %s active Globus tasks" % status_numActive)

    activateEndpoints()

    # Create thread for service to begin monitoring log file & transfer queue
    logger.info("*** Service now monitoring gantry transfer queue ***")
    thread.start_new_thread(globusMonitorLoop, ())
    logger.info("*** Service now monitoring existing Globus transfers ***")
    thread.start_new_thread(globusCleanupLoop, ())

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    logger.info("*** API now listening on %s:%s ***" % (config['api']['ip_address'],apiPort))
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

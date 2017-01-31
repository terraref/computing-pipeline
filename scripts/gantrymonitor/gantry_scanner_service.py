#!/usr/bin/env python

""" GANTRY SCANNER SERVICE
    This will load parameters from the configFile defined below,
    and begin monitoring the specified gantry file directory for
    new files.

    Several channels for appending to the pending transfer queue:
    - scanning module for "xferlog" files, walking back in time if necessary
    - SQL database monitor module
    - folder "modified in the last n minutes" scanning module

    After each scan, if any files are queued, Globus transfers will be started
    until the queue is empty or maximum number of active transfers reached.
"""

import os, shutil, json, time, datetime, thread, copy, subprocess, atexit, collections, fcntl, re, gzip, pwd
import logging, logging.config, logstash
from io import BlockingIOError

from flask import Flask, request, Response
from flask.ext import restful

from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, ClientError, goauth

rootPath = "/home/gantry"

config = {}

# Used by the FTP log reader to track progress
status_lastFTPLogLine = ""
status_lastNasLogLine = ""

app = Flask(__name__)
api = restful.Api(app)

# ----------------------------------------------------------
# OS & GLOBUS
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

"""Write monitor_status from memory into file"""
def writeStatus(filePath, taskObj):
    filePath = os.path.join(config["log_path"], "monitor_status.json")

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
    f.write(json.dumps(getStatus()))
    f.close()

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

# ----------------------------------------------------------
# POSTGRES LOGGING
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
        initializePostgresDatabase(conn)

    logger.info("Connected to Postgres")
    return conn

"""Create PostgreSQL database tables"""
def initializePostgresDatabase(db_connection):
    # Table creation queries
    ct_pending = "CREATE TABLE pending_tasks (id SERIAL, contents JSON);"
    ct_tasks = "CREATE TABLE globus_tasks (globus_id TEXT PRIMARY KEY NOT NULL, status TEXT NOT NULL, " \
               "started TEXT NOT NULL, completed TEXT, " \
               "file_count INT, bytes BIGINT, globus_user TEXT, contents JSON);"

    # Index creation queries
    ix_pending = "CREATE UNIQUE INDEX pending_idx ON pending_tasks (id);"
    ix_tasks = "CREATE UNIQUE INDEX globus_idx ON globus_tasks (globus_id);"

    # Execute each query
    curs = db_connection.cursor()
    logger.info("Creating PostgreSQL tables...")
    curs.execute(ct_pending)
    curs.execute(ct_tasks)
    logger.info("Creating PostgreSQL indexes...")
    curs.execute(ix_pending)
    curs.execute(ix_tasks)
    curs.close()
    db_connection.commit()

    logger.info("PostgreSQL initialization complete.")

"""Get at most 100 Globus batches that haven't been sent yet"""
def readPendingTasks():
    """PENDING (have group of files; not yet created Globus transfer)"""
    q_fetch = "SELECT TOP(100) id, contents FROM pending_tasks"
    results = []

    curs = psql_conn.cursor()
    curs.execute(q_fetch)
    for result in curs:
        results.append({
            "id": result[0],
            "contents": result[1]
        })
    curs.close()

    return results

"""Write a pending Globus task into PostgreSQL"""
def writePendingTaskToDatabase(task):
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
    jbody = json.dumps(task['contents'])

    # Attempt to insert, update if globus ID already exists
    q_insert = "INSERT INTO pending_tasks (contents) VALUES ('%s')" % jbody

    curs = psql_conn.cursor()
    #logger.debug("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Write a Globus task into PostgreSQL, insert/update as needed"""
def writeTaskToDatabase(task):
    """activeTasks tracks Globus transfers is of the format:
    {"globus_id": {
        "globus_id":                globus job ID of upload
        "contents": {...},          a pendingTransfers object that was sent (see above)
        "started":                  timestamp when task was sent to Globus
        "completed":                timestamp when task was completed (including errors and cancelled tasks)
        "status":                   can be "IN PROGRESS", "DONE", "ABORTED", "ERROR"
    }, {...}, {...}, ...}"""

    gid = task['globus_id']
    stat = task['status']
    start = task['started']
    comp = task['completed'] if 'completed' in task else ''
    guser = task['user'] if 'user' in task else ''
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

"""Drop pending task from database once sent"""
def removePendingTask(task_id):
    """PENDING (have group of files; not yet created Globus transfer)"""
    q_fetch = "DELETE FROM pending_tasks WHERE id='%s'" % task_id
    curs = psql_conn.cursor()
    curs.execute(q_fetch)
    curs.close()

"""Get all active Globus transfers that haven't been verified complete from Globus yet"""
def getPendingTransferCount():
    pending_count = 0

    # Get list of running tasks, whether NCSA notified or not
    q_fetch = "SELECT COUNT(id) FROM pending_tasks"

    curs = psql_conn.cursor()
    curs.execute(q_fetch)
    for result in curs:
        pending_count = result[0]
    curs.close()

    return pending_count

"""Get all active Globus transfers that haven't been verified complete from Globus yet"""
def getActiveTransferCount():
    active_count = 0

    # Get list of running tasks, whether NCSA notified or not
    q_fetch = "SELECT COUNT(globus_id) FROM globus_tasks WHERE (status = 'CREATED' OR status = 'IN PROGRESS')"

    curs = psql_conn.cursor()
    curs.execute(q_fetch)
    for result in curs:
        active_count = result[0]
    curs.close()

    return active_count

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
        new_xfers = {}

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
            new_xfers[datasetname] = newTransfer
            logger.info("- file queued via API: %s" % p)

        # Multiple file path entries under 'paths'
        if 'paths' in req:
            if spaceid:
                new_xfers['space_id'] = spaceid
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
                new_xfers = updateNestedDict(new_xfers, newTransfer)
                logger.info("- file queued via API: %s" % p)

        new_xfer_record = buildGlobusBundle(new_xfers)
        writePendingTaskToDatabase(new_xfer_record)
        return 201

""" /status
Return basic information about monitor for health checking"""
class MonitorStatus(restful.Resource):

    def get(self):
        return getStatus(), 200

api.add_resource(TransferQueue, '/files')
api.add_resource(MonitorStatus, '/status')

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
"""Return small JSON object with information about monitor health"""
def getStatus():
    global status_lastFTPLogLine, status_lastNasLogLine

    return {
        "pending_file_transfers": getPendingTransferCount(),
        "active_globus_tasks": getActiveTransferCount(),
        "last_ftp_log_line_read": status_lastFTPLogLine,
        "last_nas_log_line_read": status_lastNasLogLine
    }

"""Check FTP log files to determine new files that were successfully moved to staging area"""
def getNewFilesFromFTPLogs():
    global status_lastFTPLogLine, status_lastNasLogLine
    current_lastFTPLogLine = status_lastFTPLogLine
    current_lastNasLogLine = status_lastNasLogLine

    foundFiles = []
    logDir = config["gantry"]["ftp_log_path"]
    if logDir == "":
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
        if nasfound > 0:
            logger.info(" - found %s lines from naslog" % nasfound)

    # XFERLOG -  archived files are by date, e.g. "xferlog-20160501", "xferlog-20160502" --------------------------
    # Tue Apr  5 12:35:58 2016 1 ::ffff:150.135.84.81 4061858 /gantry_data/LemnaTec/EnvironmentLogger/2016-04-05/2016-04-05_12-34-58_enviromentlogger.json b _ i r lemnatec ftp 0 * c
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

        # If we didn't find last line in this file, look into the previous file
        if not foundResumePoint:
            handledBackLog = False
            backLog += 1

            if abs(backLog) > len(lognames):
                # No previous logs, so just start with current one
                currLog = os.path.join(logDir, "xferlog")
                foundResumePoint = True
                backLog = 0

                logger.error("LAST READ LINE NOT FOUND!")
                handledBackLog = True
            else:
                currLog = os.path.join(logDir, lognames[-backLog])

            logger.debug("- walking back to %s" % currLog)

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

"""Check folders in config for files older than the configured age, and queue for transfer"""
def getNewFilesFromWatchedFolders():
    foundFiles = []

    # Get list of files last modified more than X minutes ago
    watchDirs = config['gantry']['file_age_monitor_paths']
    fileAge = config['gantry']['min_file_age_for_transfer_mins']

    if len(watchDirs) > 0:
        for currDir in watchDirs:
            # TODO: Check for hidden files beginning with "." or just allow them?
            foundList = subprocess.check_output(["find", currDir, "-mmin", "+"+fileAge, "-type", "f", "-print"]).split("\n")
            for f in foundList:
                foundFiles.append(f)
        logger.info("- found %s files from watched folders" % len(foundFiles))

    return foundFiles

"""Query SQL database for new records"""
def getNewFilesFromSQLDatabase():
    pass

"""Create object for the globus_tasks table, even before transmission"""
def buildGlobusBundle(queued_files):
    new_bundle = {}

    for ds in queued_files:
        if "files" in queued_files[ds]:
            # Add dataset if this is first file from it
            if ds not in new_bundle:
                new_bundle[ds] = {}
                new_bundle[ds]['files'] = {}
            # Add files from each dataset
            for f in queued_files[ds]['files']:
                fobj = queued_files[ds]['files'][f]
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

                fobj["orig_path"] = fobj["path"]
                fobj["path"] = dest_path
                fobj['src_path'] = src_path
                new_bundle[ds]['files'][f] = fobj

            # Clean up any placeholder entries
            if new_bundle[ds]['files'] == {}:
                del new_bundle[ds]['files']
            if new_bundle[ds] == {}:
                del new_bundle[ds]

    return new_bundle

"""Make entry for pendingTransfers"""
def prepFileForPendingTransfers(f, sensorname=None, timestamp=None, datasetname=None, filemetadata={}, manual=False):
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
    # Dataset typically is "sensorName - YYYY-MM-DD__hh-mm-ss-mms"
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

"""Check for files ready for transmission and queue them"""
def queueGantryFilesIntoPending():
    logger.debug("- scanning for new files to transfer")
    max_xfer_size = config['globus']['max_transfer_file_count']

    # Get list of files from FTP log, if log is specified
    foundFiles = getNewFilesFromFTPLogs()

    # Get list of files from watched folders, if folders are specified  (and de-duplicate from FTP list)
    fileList = getNewFilesFromWatchedFolders()
    for found in fileList:
        if found not in foundFiles:
            foundFiles.append(found)

    logger.debug("- adding %s found files into pending tasks" % len(foundFiles))
    new_xfer_count = 0
    try:
        new_xfers = {}
        queue_size = 0
        for f in foundFiles:
            # Skip hidden/system files
            if f == "" or f.split("/")[-1][0] == ".":
                continue
            new_xfers = updateNestedDict(new_xfers, prepFileForPendingTransfers(f))
            queue_size += 1

            # Create pending Globus xfers every n files for database entry
            if queue_size >= max_xfer_size:
                new_xfer_record = buildGlobusBundle(new_xfers)
                writePendingTaskToDatabase(new_xfer_record)
                new_xfers = {}
                queue_size = 0
                new_xfer_count += 1

    except Exception as e:
        logger.error("problem adding files: %s" % str(e))
    logger.debug("- added %s entries to pending tasks" % new_xfer_count)

"""Initiate Globus transfer with batch of files and add to activeTasks - recurse until max xfers reached or pending empty """
def initializeGlobusTransfer(globus_batch_obj):
    globus_batch = globus_batch_obj['contents']
    globus_batch_id = globus_batch_obj['id']


    api = TransferAPIClient(username=config['globus']['username'], goauth=config['globus']['auth_token'])
    submissionID = generateGlobusSubmissionID()

    if submissionID:
        # Prepare transfer object
        transferObj = Transfer(submissionID,
                               config['globus']['source_endpoint_id'],
                               config['globus']['destination_endpoint_id'],
                               verify_checksum=True)
        queue_length = 0
        for ds in globus_batch:
            if 'files' in globus_batch[ds]:
                for f in globus_batch[ds]['files']:
                    transferObj.add_item(f['src_path'], f['path'])
                    queue_length += 1

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
            logger.info("%s new Globus transfer task started (%s files)" % (globusID, queue_length), extra={
                "globus_id": globusID,
                "action": "TRANSFER STARTED",
                "contents": globus_batch
            })

            created_task = {
                "globus_id": globusID,
                "contents": globus_batch,
                "started": str(datetime.datetime.now()),
                "status": "CREATED"
            }
            writeTaskToDatabase(created_task)
            removePendingTask(globus_batch_id)
        else:
            # If failed, leave pending list as-is and try again on next iteration (e.g. in 180 seconds)
            logger.error("- Globus transfer initialization failed for %s (%s: %s)" % (ds, status_code, status_message))
            return

"""Continually initiate transfers from pending queue and contact NCSA API for status updates"""
def globusInitializerLoop():
    # Prepare timers for tracking how often different refreshes are executed
    globusWait = config['gantry']['globus_transfer_frequency_secs'] # bundle pending files and transfer
    authWait = config['globus']['authentication_refresh_frequency_secs'] # renew globus auth

    while True:
        time.sleep(1)
        globusWait -= 1
        authWait -= 1

        # Check pending queue and initiate transfers if ready
        if globusWait <= 0:
            active_count = getActiveTransferCount()
            if active_count < config["globus"]["max_active_tasks"]:
                logger.debug("- currently %s active tasks; starting more from pending queue" % active_count)

                pending_tasks = readPendingTasks()
                for p in pending_tasks:
                    if active_count < config["globus"]["max_active_tasks"]:
                        initializeGlobusTransfer(p)
                        active_count += 1
                    else:
                        break

            # Reset wait to check gantry incoming directory again
            globusWait = config['gantry']['globus_transfer_frequency_secs']
            writeStatus()

        # Refresh Globus auth tokens
        if authWait <= 0:
            generateAuthToken()
            authWait = config['globus']['authentication_refresh_frequency_secs']

"""Continually monitor FTP log for new files to transmit and add them to pendingTransfers"""
def gantryMonitorLoop():
    gantryWait = 1 # look for new files to send

    while True:
        time.sleep(1)
        gantryWait -= 1

        # Check for new files in incoming gantry directory and initiate transfers if ready
        if gantryWait <= 0:
            queueGantryFilesIntoPending()
            writeStatus()
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
        main_log_file = os.path.join(config["log_path"], "log_scanner.txt")
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
    activateEndpoints()

    # Create thread for service to begin monitoring log file & transfer queue
    logger.info("*** Service now monitoring pending transfer queue ***")
    thread.start_new_thread(globusInitializerLoop, ())
    logger.info("*** Service now checking for new files via FTP logs/folder monitoring ***")
    thread.start_new_thread(gantryMonitorLoop, ())

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    logger.info("*** API now listening on %s:%s ***" % (config['api']['ip_address'],apiPort))
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

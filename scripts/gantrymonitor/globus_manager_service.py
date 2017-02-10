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
from dateutil.parser import parse
import psycopg2

from flask import Flask, request, Response
from flask.ext import restful

from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, ClientError, goauth

from influxdb import InfluxDBClient, SeriesHelper


rootPath = "/home/gantry"

config = {}

app = Flask(__name__)
api = restful.Api(app)

# ----------------------------------------------------------
# OS & GLOBUS
# ----------------------------------------------------------
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

"""Query Globus API to get current transfer status of a given task"""
def getGlobusTaskData(task):
    authToken = config['globus']['valid_users'][task['user']]['auth_token']
    api = TransferAPIClient(username=task['user'], goauth=authToken)
    try:
        logger.debug("%s requesting task data from Globus" % task['globus_id'])
        status_code, status_message, task_data = api.task(task['globus_id'])
    except (APIError, ClientError) as e:
        try:
            # Refreshing auth tokens and retry
            generateAuthToken()
            authToken = config['globus']['valid_users'][task['user']]['auth_token']
            api = TransferAPIClient(username=task['user'], goauth=authToken)
            status_code, status_message, task_data = api.task(task['globus_id'])
        except (APIError, ClientError) as e:
            logger.error("%s error checking with Globus for transfer status" % task['globus_id'])
            status_code = 503

    if status_code == 200:
        return task_data
    else:
        return None

# ----------------------------------------------------------
# POSTGRES & INFLUXDB LOGGING
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
        return psycopg2.connect(dbname='globusmonitor')
    except:
        logger.info("Could not connect to globusmonitor Postgres database.")
        return None

"""Fetch all Globus tasks with a particular status"""
def readTasksByStatus(status, id_only=False):
    """
        CREATED (initialized transfer; not yet notified NCSA side)
    IN PROGRESS (notified of transfer; but not yet verified complete)
         FAILED (Globus could not complete; no longer attempting to complete)
        DELETED (manually via api)
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

"""Write a Globus task into PostgreSQL, insert/update as needed"""
def writeTaskToPostgres(task):
    """A task object tracks Globus transfers is of the format:
    {"globus_id": {
        "globus_id":                globus job ID of upload
        "contents": {...},          a pendingTransfers object that was sent (see below)
        "started":                  timestamp when task was sent to Globus
        "completed":                timestamp when task was completed (including errors and cancelled tasks)
        "status":                   see readTasksByStatus for options
    }, {...}, {...}, ...}
    ---------------------------------
    "contents" internal structure:
    "contents": {
        "dataset": {
            "files": {
                "filename1": {
                    "name": "filename1",
                    "path": path on NCSA destination side
                    "orig_path": path on gantry
                    "src_path": path on gantry, corrected for Globus mounts
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
    gid = task['globus_id']
    stat = task['status']
    start = task['started']
    comp = task['completed']
    guser = task['user']
    filecount = int(task['file_count']) if 'file_count' in task else -1
    bytecount = int(task['bytes']) if 'bytes' in task else -1
    jbody = json.dumps(task['contents'])

    # Attempt to insert, update if globus ID already exists
    q_insert_95 = "INSERT INTO globus_tasks (globus_id, status, started, completed, globus_user, file_count, bytes, contents) " \
               "VALUES ('%s', '%s', '%s', '%s', '%s', %s, %s, '%s') " \
               "ON CONFLICT (globus_id) DO UPDATE " \
               "SET status='%s', started='%s', completed='%s', globus_user='%s', file_count=%s, bytes=%s, contents='%s';" % (
                   gid, stat, start, comp, guser, filecount, bytecount, jbody, stat, start, comp, guser, filecount, bytecount, jbody)

    # Alternate query for < PGSQL 9.5 (no ON CONFLICT support)
    q_insert_94 = """
    BEGIN;

    CREATE TEMPORARY TABLE newvals(globus_id TEXT PRIMARY KEY NOT NULL, status TEXT NOT NULL,
                started TEXT NOT NULL, completed TEXT,
                file_count INT, bytes BIGINT, globus_user TEXT, contents JSON);

    INSERT INTO newvals(globus_id, status, started, completed, globus_user, file_count, bytes, contents)
    VALUES ('%s', '%s', '%s', '%s', '%s', %s, %s, '%s');

    LOCK TABLE globus_tasks IN EXCLUSIVE MODE;

    UPDATE globus_tasks
    SET status=newvals.status, started=newvals.started, completed=newvals.completed, globus_user=newvals.globus_user,
                file_count=newvals.file_count, bytes=newvals.bytes, contents=newvals.contents
    FROM newvals
    WHERE newvals.globus_id = globus_tasks.globus_id;

    INSERT INTO globus_tasks
    SELECT newvals.globus_id, newvals.status, newvals.started, newvals.completed, newvals.globus_user,
            newvals.file_count, newvals.bytes, newvals.contents
    FROM newvals
    LEFT OUTER JOIN globus_tasks ON (globus_tasks.globus_id = newvals.globus_id)
    WHERE globus_tasks.id IS NULL;

    COMMIT;
    """ % (gid, stat, start, comp, guser, filecount, bytecount, jbody)

    curs = psql_conn.cursor()
    #logger.debug("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert_94)
    psql_conn.commit()
    curs.close()

"""Insert or update record for Influx tracking, since Influx doesn't support updates"""
def writePointToPostgres(dsname, file_ct, byte_ct, create_time, xfer_time):
    q_insert = "INSERT INTO dataset_logs (name, filecount, bytecount, created, transferred) " \
               "VALUES ('%s', %s, %s, %s, %s) " \
               "ON CONFLICT (name) DO UPDATE " \
               "SET filecount=filecount+%s, bytecount=bytecount+%s, tranferred=MAX(transferred, %s);" % (
                   dsname, file_ct, byte_ct, create_time, xfer_time)

    curs = psql_conn.cursor()
    #logger.debug("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert)
    psql_conn.commit()

    # Get current counts
    currPoint = None
    q_fetch = "SELECT filecount, bytecount, created, transferred " \
              "FROM dataset_logs WHERE name='%s'" % dsname
    curs.execute(q_fetch)
    for result in curs:
        currPoint = {
            "name": dsname,
            "filecount": result[0],
            "bytecount": result[1],
            "created": result[2],
            "transferred": result[3]
        }
    curs.close()

    return currPoint

"""Iterate through files in a task and write them to InfluxDB"""
def writeTaskToInflux(task):
    """Following columns in InfluxDB:
        - filename
        - bytes (size of file)
        - sensor
        - date (YYYY-MM-DD of dataset)
        - timestamp (HH-MM-SS-mms of dataset if available)
        - gid (globus ID of transfer in which file was sent)
        - completed (timestamp when globus transfer was completed
    """
    gid = task['globus_id']
    comp = task['completed']
    site_time = config['influx']['site_time']

    client = InfluxDBClient(config['influx']['host'],
                            config['influx']['port'],
                            config['influx']['username'],
                            config['influx']['password'],
                            config['influx']['dbname'])

    influxPoints = []

    # Walk transfer object and determine data on each file
    dataset_by_date = False
    for ds in task['contents']:
        fsensor = ds.split(" - ")[0]
        fdate = ds.split(" - ")[1]
        if fdate.find("__") > -1:
            ftime = fdate.split("__")[1][:8].replace("-",":") + site_time
            fdate = fdate.split("__")[0]
        else:
            dataset_by_date = True

        ds_file_count = 0
        ds_byte_count = 0

        if 'files' in task['contents'][ds]:
            for f in task['contents'][ds]['files']:
                fname = f['name']
                fsize = os.stat(f['orig_path']).st_size

                if dataset_by_date:
                    # Dataset is by DATE level, so get timestamp from filename if possible
                    if fname.find("environmentlogger") > -1:
                        # ../2016-04-05_12-34-58_enviromentlogger.json
                        ftime = fname.split("_")[1].replace("-",":") + site_time
                    elif fname.find(".dat") > -1:
                        # ../weather_2016_06_29.dat
                        ftime = "12:00:00" + site_time

                # InfluxDB accepts time in seconds from epoch
                f_created_ts = int(parse(fdate+"T"+ftime).strftime('%s'))
                # completion time from Globus is formatted: "2017-02-10 16:09:57+00:00"
                f_transferred_ts = int(parse(comp.replace(" ", "T")).strftime('%s'))

                # DATASET | BYTES | FILECOUNT | CREATE_TIME | COMPLETE_TIME
                ds_file_count += 1
                ds_byte_count += fsize

                influxPoints.append({
                    "measurement": "file_create",
                    "time": f_created_ts,
                    "fields": {"sensor": fsensor, "type": "bytes", "count": fsize}
                })
                influxPoints.append({
                    "measurement": "file_create",
                    "time": f_created_ts,
                    "fields": {"sensor": fsensor, "type": "filecount", "count": 1}
                })
                influxPoints.append({
                    "measurement": "file_transfer",
                    "time": f_transferred_ts,
                    "fields": {"sensor": fsensor, "type": "bytes", "count": fsize}
                })
                influxPoints.append({
                    "measurement": "file_transfer",
                    "time": f_transferred_ts,
                    "fields": {"sensor": fsensor, "type": "filecount", "count": 1}
                })

                pointTotal = writePointToPostgres(ds, ds_file_count, ds_byte_count, f_created_ts, f_transferred_ts)

    # Post points to Influx database
    client.write_points([{

    }])

# ----------------------------------------------------------
# SERVICE COMPONENTS
# ----------------------------------------------------------
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
        return {
            'status_code': status.status_code
        }

    except requests.ConnectionError as e:
        logger.error("- cannot connect to NCSA API")
        return {'status_code':503}

"""Continually initiate transfers from pending queue and contact NCSA API for status updates"""
def globusMonitorLoop():
    global activeTasks

    # Prepare timers for tracking how often different refreshes are executed
    apiWait = 1 # check status of sent files
    authWait = config['globus']['authentication_refresh_frequency_secs'] # renew globus auth

    while True:
        time.sleep(1)
        apiWait -= 1
        authWait -= 1

        if apiWait <= 0:
            sess = requests.Session()
            sess.auth = (config['globus']['username'], config['globus']['password'])

            logger.debug("- attempting to notify NCSA of unfamiliar Globus tasks")

            # CREATED -> IN PROGRESS on NCSA notification
            current_tasks = readTasksByStatus("CREATED")
            for taskid in current_tasks:
                task = current_tasks[taskid]
                notify = notifyMonitorOfNewTransfer(taskid, task['contents'], sess)
                if notify['status_code'] == 200:
                    task['status'] = "IN PROGRESS"
                    writeTaskToPostgres(task)

            # SUCCEEDED -> NOTIFIED on NCSA notification
            current_tasks = readTasksByStatus("SUCCEEDED")
            for taskid in current_tasks:
                task = current_tasks[taskid]
                notify = notifyMonitorOfNewTransfer(taskid, task['contents'], sess)
                if notify['status_code'] == 200:
                    task['status'] = "NOTIFIED"
                    writeTaskToPostgres(task)

            logger.debug("- attempting to contact Globus for transfer status updates")

            # CREATED -> SUCCEEDED on completion, NCSA not yet notified
            #         -> FAILED on failure
            current_tasks = readTasksByStatus("CREATED")
            for taskid in current_tasks:
                task = current_tasks[taskid]
                task_data = getGlobusTaskData(task)
                if task_data and task_data['status'] in ["SUCCEEDED", "FAILED"]:
                    task['status'] = task_data['status']
                    task['started'] = task_data['request_time']
                    task['completed'] = task_data['completion_time']
                    task['file_count'] = task_data['files']
                    task['bytes'] = task_data['bytes_transferred']
                    writeTaskToPostgres(task)
                    writeTaskToInflux(task)

            # IN PROGRESS -> NOTIFIED on completion, NCSA already notified
            #             -> FAILED on failure
            current_tasks = readTasksByStatus("IN PROGRESS")
            for taskid in current_tasks:
                task = current_tasks[taskid]
                task_data = getGlobusTaskData(task)
                if task_data and task_data['status'] in ["SUCCEEDED", "FAILED"]:
                    task['status'] = "NOTIFIED" if task_data['status'] == "SUCCEEDED" else "FAILED"
                    task['started'] = task_data['request_time']
                    task['completed'] = task_data['completion_time']
                    task['file_count'] = task_data['files']
                    task['bytes'] = task_data['bytes_transferred']
                    writeTaskToPostgres(task)
                    writeTaskToInflux(task)

            apiWait = config['ncsa_api']['api_check_frequency_secs']

        # Refresh Globus auth tokens
        if authWait <= 0:
            generateAuthToken()
            authWait = config['globus']['authentication_refresh_frequency_secs']


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

    # Connect to Postgres & start processing
    psql_conn = connectToPostgres()
    if psql_conn:
        activateEndpoints()

        logger.info("*** Service now monitoring existing Globus transfers ***")
        globusMonitorLoop()

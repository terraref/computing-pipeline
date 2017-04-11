#!/usr/bin/python

""" GLOBUS MONITOR SERVICE
    This will load parameters from the configFile defined below,
    and start up an API to listen on the specified port for new
    Globus task IDs. It will then monitor the specified Globus
    directory and query the Globus API until that task ID has
    succeeded or failed, and update Postgres accordingly.
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
    return {
        "in_progress_tasks": countTasksByStatus("IN PROGRESS"),
        "unprocessed_tasks": countTasksByStatus("SUCCEEDED"),
        "processed_tasks": countTasksByStatus("PROCESSED"),
        "errored_tasks": countTasksByStatus("ERROR")
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
    ct_tasks = "CREATE TABLE globus_tasks (globus_id TEXT PRIMARY KEY NOT NULL, status TEXT NOT NULL, " \
               "received TEXT NOT NULL, completed TEXT, " \
               "file_count INT, bytes BIGINT, globus_user TEXT, contents JSON);"
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
   q_fetch = "SELECT globus_id, status, received, completed, globus_user, " \
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
           "received": result[2],
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
    recv = task['received']
    comp = task['completed']
    guser = task['user']
    filecount = int(task['file_count']) if 'file_count' in task else -1
    bytecount = int(task['bytes']) if 'bytes' in task else -1
    jbody = json.dumps(task['contents'])

    # Attempt to insert, update if globus ID already exists
    q_insert = "INSERT INTO globus_tasks (globus_id, status, received, completed, globus_user, file_count, bytes, contents) " \
               "VALUES ('%s', '%s', '%s', '%s', '%s', %s, %s, '%s') " \
               "ON CONFLICT (globus_id) DO UPDATE " \
               "SET status='%s', received='%s', completed='%s', globus_user='%s', file_count=%s, bytes=%s, contents='%s';" % (
        gid, stat, recv, comp, guser, filecount, bytecount, jbody, stat, recv, comp, guser, filecount, bytecount, jbody)

    curs = psql_conn.cursor()
    #logger.debug("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert)
    psql_conn.commit()
    curs.close()

"""Fetch all Globus tasks with a particular status"""
def readTasksByStatus(status, id_only=False, limit=2500):
    """
    IN PROGRESS (received notification from sender but not yet verified complete)
         FAILED (Globus could not complete; no longer attempting to complete)
        DELETED (manually via api below)
      SUCCEEDED (verified complete; not yet uploaded into Clowder)
      PROCESSED (complete & uploaded into Clowder)
          ERROR (encountered error uploading into Clowder)
    """
    if id_only:
        q_fetch = "SELECT globus_id FROM globus_tasks WHERE status = '%s' limit %s;" % (status, limit)
        results = []
    else:
        q_fetch = "SELECT globus_id, status, received, completed, globus_user, " \
                  "file_count, bytes, contents FROM globus_tasks WHERE status = '%s' limit %s;" % (status, limit)
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
                "received": result[2],
                "completed": result[3],
                "user": result[4],
                "file_count": result[5],
                "bytes": result[6],
                "contents": result[7]
            }
    curs.close()

    return results

"""Count all Globus tasks with a particular status"""
def countTasksByStatus(status):
    """
    IN PROGRESS (received notification from sender but not yet verified complete)
         FAILED (Globus could not complete; no longer attempting to complete)
        DELETED (manually via api below)
      SUCCEEDED (verified complete; not yet uploaded into Clowder)
      PROCESSED (complete & uploaded into Clowder)
    """
    q_fetch = "SELECT count(1) FROM globus_tasks WHERE status = '%s';" % status
    count = -1

    curs = psql_conn.cursor()
    #logger.debug("Fetching all %s tasks from PostgreSQL..." % status)
    curs.execute(q_fetch)
    for result in curs:
        count = result[0]
    curs.close()

    return count

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

    """Return list of first 10 active tasks initiated by the requesting user"""
    @requires_auth
    def get(self):
        # TODO: Should this be filtered by user somehow?
        return readTasksByStatus("IN PROGRESS", limit=10), 200

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
        task = readTaskFromDatabase(globusID)
        if task:
            return task, 200
        else:
            return "Globus ID not found in database", 404

""" /status
Return basic information about monitor for health checking"""
class MonitorStatus(restful.Resource):

    def get(self):
        return getStatus(), 200

api.add_resource(GlobusMonitor, '/tasks')
api.add_resource(GlobusTask, '/tasks/<string:globusID>')
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
def getGlobusTaskData(task):
    authToken = config['globus']['valid_users'][task['user']]['auth_token']
    api = TransferAPIClient(username=task['user'], goauth=authToken)
    try:
        logger.debug("%s requesting task data from Globus" % task['globus_id'])
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
        return task_data
    else:
        return None

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
            logger.info("- checking for Globus updates")
            # Use copy of task list so it doesn't change during iteration
            activeTasks = readTasksByStatus("IN PROGRESS")
            """activeTasks tracks which Globus IDs are being monitored, and is of the format:
                {"globus_id": {
                    "user":                     globus username
                    "globus_id":                globus job ID of upload
                    "contents": {
                        "dataset": {                dataset used as key for set of files transferred
                            "md": {}                metadata to be associated with this dataset
                            "files": {              dict of files from this dataset included in task, each with
                                "filename1___extension": {
                                    "name": "file1.txt",    ...filename
                                    "path": "",             ...file path, which is updated with path-on-disk once completed
                                    "md": {}                ...metadata to be associated with that file
                                    "clowder_id": "UUID"    ...UUID of file in Clowder once it is uploaded
                                },
                                "filename2___extension": {...},
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
            for globusID in activeTasks:
                task = activeTasks[globusID]
                task_data = getGlobusTaskData(task)

                if task_data:
                    globusStatus = task_data['status']
                    task['received'] = task_data['request_time']
                    task['file_count'] = task_data['files']
                    task['bytes'] = task_data['bytes_transferred']

                    logger.info("%s status received: %s" % (globusID, globusStatus), extra={
                        "globus_id": globusID,
                        "status": globusStatus,
                        "action": "STATUS UPDATED"
                    })

                    # If this isn't done yet, leave the task active so we can try again next time
                    if globusStatus in ["SUCCEEDED", "FAILED"]:
                        # Update task parameters
                        task['status'] = globusStatus
                        task['completed'] = task_data['completion_time']

                        # Update task file paths
                        for ds in task['contents']:
                            if 'files' in task['contents'][ds]:
                                for f in task['contents'][ds]['files']:
                                    fobj = task['contents'][ds]['files'][f]
                                    fobj['path'] = os.path.join(config['globus']['incoming_files_path'], fobj['path'])

                        writeTaskToDatabase(task)

            logger.debug("- done checking for Globus updates")

            globWait = 0
            writeStatusToDisk()

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

    psql_conn = connectToPostgres()
    generateAuthTokens()

    logger.info("- initializing service")
    # Create thread for service to begin monitoring
    thread.start_new_thread(globusMonitorLoop, ())
    logger.info("*** Service now monitoring Globus tasks ***")

    # Create thread for API to begin listening - requires valid Globus user/pass
    apiPort = os.getenv('MONITOR_API_PORT', config['api']['port'])
    logger.info("*** API now listening on %s:%s ***" % (config['api']['ip_address'], apiPort))
    app.run(host=config['api']['ip_address'], port=int(apiPort), debug=False)

import os, json
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

"""Load contents of .json file into a JSON object"""
def loadJsonFile(filename):
    try:
        f = open(filename)
        jsonObj = json.load(f)
        f.close()
        return jsonObj
    except IOError:
        print("- unable to open %s" % filename)
        return {}

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
    print("Creating PostgreSQL tables...")
    curs.execute(ct_tasks)
    curs.execute(ct_dsets)
    curs.execute(ct_colls)
    print("Creating PostgreSQL indexes...")
    curs.execute(ix_tasks)
    curs.execute(ix_dsets)
    curs.execute(ix_colls)
    curs.close()
    db_connection.commit()

    print("PostgreSQL initialization complete.")
    
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
    #print("Writing task %s to PostgreSQL..." % gid)
    curs.execute(q_insert)
    curs.close()

"""Write dataset (name -> clowder_id) mapping to PostgreSQL database"""
def writeDatasetRecordToDatabase(dataset_name, dataset_id):

    q_insert = "INSERT INTO datasets (name, clowder_id) VALUES ('%s', '%s') " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id='%s';" % (
        dataset_name, dataset_id, dataset_id)

    curs = psql_conn.cursor()
    #print("Writing dataset %s to PostgreSQL..." % dataset_name)
    curs.execute(q_insert)
    curs.close()

"""Write collection (name -> clowder_id) mapping to PostgreSQL database"""
def writeCollectionRecordToDatabase(collection_name, collection_id):

    q_insert = "INSERT INTO collections (name, clowder_id) VALUES ('%s', '%s') " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id='%s';" % (
        collection_name, collection_id, collection_id)

    curs = psql_conn.cursor()
    #print("Writing collection %s to PostgreSQL..." % collection_name)
    curs.execute(q_insert)
    curs.close()

# ERROR FILES
# completed/79/7e/d0/40/797ed040-745e-11e6-8427-22000b97daec.json
# completed/d4/72/8c/ae/d4728cae-7458-11e6-8427-22000b97daec.json
# completed/dc/7b/c7/e4/dc7bc7e4-899d-11e6-b030-22000b92c261.json
# completed/f2/b8/9d/30/f2b89d30-1872-11e6-a7cf-22000bf2d559.json
#--------------------------------------------------------------------------
root = "/home/globusmonitor/computing-pipeline/scripts/globusmonitor/data"
#--------------------------------------------------------------------------

psql_conn = connectToPostgres()
commLoop = 0

# Handle ID maps first
print("Loading dataset map into Postgres...")
ds_json = loadJsonFile(os.path.join(root, 'log/datasets.json'))
for key in ds_json:
    writeDatasetRecordToDatabase(key, ds_json[key])
    commLoop += 1
    if commLoop % 10000 == 0:
        psql_conn.commit()
del ds_json

print("Loading collection map into Postgres...")
coll_json = loadJsonFile(os.path.join(root, 'log/collections.json'))
for key in coll_json:
    writeDatasetRecordToDatabase(key, coll_json[key])
    commLoop += 1
    if commLoop % 10000 == 0:
        psql_conn.commit()
del coll_json
    
# Now handle tasks
print("Loading completed.json files into Postgres...")
completed = os.path.join(root, 'completed')
unproc_list = loadJsonFile(os.path.join(root, 'log/unprocessed_tasks.txt'))
active_list = loadJsonFile(os.path.join(root, 'log/active_tasks.json'))
count = 0
for root, dirs, files in os.walk(completed):
    for f in files:
        if f.endswith(".json"):
            fpath = os.path.join(root, f)

            try:
                taskdata = loadJsonFile(fpath)
                gid = taskdata['globus_id']
                if gid in active_list.keys(): taskdata['status'] = 'IN PROGRESS'
                elif gid not in unproc_list:  taskdata['status'] = 'PROCESSED'
                writeTaskToDatabase(taskdata)
                count += 1
            except ValueError:
                print("...no JSON object decoded in %s" % fpath)
        if count % 1000 == 0:
            print("...loaded %s files" % count)
            psql_conn.commit()

print("Data load complete.")



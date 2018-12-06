import os
import datetime
import json
import requests

from pyclowder.connectors import Connector
from pyclowder.datasets import upload_metadata, create_empty as create_empty_dataset
from pyclowder.files import upload_to_dataset
from pyclowder.collections import create_empty as create_empty_collection
from terrautils.metadata import clean_metadata


# -----
# BEFORE RUNNING:
#   source ~/pyupdates/bin/activate
# -----


# CONNECTION SETTINGS
CLOWDER_HOST = "http://terraref.ncsa.illinois.edu/clowder/"
CLOWDER_USER = "mburnet2@illinois.edu"
CONN = Connector(None, mounted_paths={"/home/clowder/sites":"/home/clowder/sites"})

SPACE_ID = "599d856b4f0c19c55fba8031"
SENSOR_FOLDER = "/home/clowder/sites/ua-mac/raw_data"
LOGFILE = open("build_log.txt", "w")
SENSOR_LIST = ["stereoTop"]
DAY_LIST = ["2017-06-15", "2017-07-30"]
TIMESTAMP_FOLDER = True
DRY_RUN = False
SKIP_DS_CHECK = True

DATASETS = {}
COLLECTIONS = {}


def log(string):
    print("%s: %s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), string))
    LOGFILE.write("%s: %s" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), string))

def loadJsonFile(jsonfile):
    try:
        f = open(jsonfile)
        jsonobj = json.load(f)
        f.close()
        return jsonobj
    except IOError:
        print("- unable to open %s" % jsonfile)
        return {}

def build_dataset_hierarchy(connector, host, secret_key, root_space, root_coll_name,
                            year='', month='', date='', leaf_ds_name=''):
    """This will build collections for year, month, date level if needed in parent space.

        Typical hierarchy:
        MAIN LEVEL 1 DATA SPACE IN CLOWDER
        - Root collection for sensor ("stereoRGB geotiffs")
            - Year collection ("stereoRGB geotiffs - 2017")
                - Month collection ("stereoRGB geotiffs - 2017-01")
                    - Date collection ("stereoRGB geotiffs - 2017-01-01")
                        - Dataset ("stereoRGB geotiffs - 2017-01-01__01-02-03-456")

        Omitting year, month or date will result in dataset being added to next level up.
    """
    parent_collect = get_collection_or_create(connector, host, secret_key, root_coll_name,
                                              parent_space=root_space)

    if year:
        # Create year-level collection
        collname = "%s - %s" % (root_coll_name, year)
        if collname not in COLLECTIONS:
            year_collect = get_collection_or_create(connector, host, secret_key, collname, parent_collect)
            COLLECTIONS[collname] = year_collect
        else:
            year_collect = COLLECTIONS[collname]

        if month:
            # Create month-level collection
            collname = "%s - %s-%s" % (root_coll_name, year, month)
            if collname not in COLLECTIONS:
                month_collect = get_collection_or_create(connector, host, secret_key, collname, year_collect)
                COLLECTIONS[collname] = month_collect
            else:
                month_collect = COLLECTIONS[collname]

            if date:
                collname = "%s - %s-%s-%s" % (root_coll_name, year, month, date)
                if collname not in COLLECTIONS:
                    targ_collect = get_collection_or_create(connector, host, secret_key, collname, month_collect)
                    COLLECTIONS[collname] = targ_collect
                else:
                    targ_collect = COLLECTIONS[collname]
            else:
                targ_collect = month_collect
        else:
            targ_collect = year_collect
    else:
        targ_collect = parent_collect

    if leaf_ds_name not in DATASETS:
        target_dsid = get_dataset_or_create(connector, host, secret_key, leaf_ds_name,
                                            targ_collect, root_space)
        DATASETS[leaf_ds_name] = target_dsid
    else:
        target_dsid = DATASETS[leaf_ds_name]

    return target_dsid

def get_collection_or_create(connector, host, secret_key, cname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/collections?key=%s&title=%s" % (host, secret_key, cname)
    result = requests.get(url, verify=connector.ssl_verify)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_collection(connector, host, secret_key, cname, "",
                                       parent_colln, parent_space)
    else:
        return result.json()[0]['id']

def get_dataset_or_create(connector, host, secret_key, dsname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    if SKIP_DS_CHECK:
        return create_empty_dataset(connector, host, secret_key, dsname, "",
                                    parent_colln, parent_space)

    url = "%sapi/datasets?key=%s&title=%s" % (host, secret_key, dsname)
    result = requests.get(url, verify=connector.ssl_verify)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_dataset(connector, host, secret_key, dsname, "",
                                    parent_colln, parent_space)
    else:
        return result.json()[0]['id']

def upload_ds(conn, host, key, sensor, date, timestamp, ds_files, ds_meta):
    if len(ds_files) > 0:
        year, month, dd = date.split("-")
        if DRY_RUN:
            log("[%s] %s files" % (sensor+' - '+timestamp, len(ds_files)))
            return

        if TIMESTAMP_FOLDER:
            dataset_id = build_dataset_hierarchy(CONN, CLOWDER_HOST, CLOWDER_KEY, SPACE_ID,
                                                 sensor, year, month, dd, sensor+' - '+timestamp)
        else:
            dataset_id = build_dataset_hierarchy(CONN, CLOWDER_HOST, CLOWDER_KEY, SPACE_ID,
                                                 sensor, year, month, leaf_ds_name=sensor+' - '+date)

        log("adding files to Clowder dataset %s" % dataset_id)

        for FILEPATH in ds_files:
            upload_to_dataset(CONN, CLOWDER_HOST, CLOWDER_KEY, dataset_id, FILEPATH)
        if len(ds_meta.keys()) > 0:
            log("adding metadata to Clowder dataset %s" % dataset_id)
            format_md = {
                "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld",
                             {"@vocab": "https://terraref.ncsa.illinois.edu/metadata/uamac#"}],
                "content": ds_meta,
                "agent": {
                    "@type": "cat:user",
                    "user_id": "https://terraref.ncsa.illinois.edu/clowder/api/users/58e2a7b9fe3ae3efc1632ae8"
                }
            }
            upload_metadata(CONN, CLOWDER_HOST, CLOWDER_KEY, dataset_id, format_md)


for sensor in SENSOR_LIST:
    SENSOR_DIR = os.path.join(SENSOR_FOLDER, sensor)

    for date in os.listdir(SENSOR_DIR):
        if date not in DAY_LIST:
            continue
        DATE_DIR = os.path.join(SENSOR_DIR, date)

        # Need one additional loop if there is a timestamp-level directory
        if TIMESTAMP_FOLDER and os.path.isdir(DATE_DIR):
            log("Scanning datasets in %s" % DATE_DIR)
            for timestamp in os.listdir(DATE_DIR):
                TIMESTAMP_DIR = os.path.join(DATE_DIR, timestamp)
                DS_FILES = []
                DS_META = {}

                # Find files and metadata in the directory
                for filename in os.listdir(TIMESTAMP_DIR):
                    if filename[0] != ".":
                        FILEPATH = os.path.join(TIMESTAMP_DIR, filename)
                        if filename.find("metadata.json") > -1:
                            DS_META = clean_metadata(loadJsonFile(FILEPATH), sensor)
                        else:
                            DS_FILES.append(FILEPATH)

                if DS_META == {} and sensor == "scanner3DTop":
                    ALT_DIR = TIMESTAMP_DIR.replace("Level_1", "raw_data")
                    for alt_fname in os.listdir(ALT_DIR):
                        if filename[0] != ".":
                            FILEPATH = os.path.join(TIMESTAMP_DIR, filename)
                            if filename.find("metadata.json") > -1:
                                DS_META = clean_metadata(loadJsonFile(FILEPATH), sensor)

                upload_ds(CONN, CLOWDER_HOST, CLOWDER_KEY, sensor, date, timestamp, DS_FILES, DS_META)

        # Otherwise the date is the dataset level
        elif os.path.isdir(DATE_DIR):
            log("Scanning datasets in %s" % SENSOR_DIR)
            DS_FILES = []
            DS_META = {}
            for filename in os.listdir(DATE_DIR):
                if filename[0] != ".":
                    FILEPATH = os.path.join(DATE_DIR, filename)
                    if filename.find("metadata.json") > -1:
                        DS_META = clean_metadata(loadJsonFile(FILEPATH), sensor)
                    else:
                        DS_FILES.append(FILEPATH)

            upload_ds(CONN, CLOWDER_HOST, CLOWDER_KEY, sensor, date, '', DS_FILES, DS_META)


log("Completed.")
LOGFILE.close()

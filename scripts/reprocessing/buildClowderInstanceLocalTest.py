import os
import datetime
import json

from pyclowder.connectors import Connector
from pyclowder.datasets import upload_metadata
from pyclowder.files import upload_to_dataset
from terrautils.extractors import build_dataset_hierarchy, build_metadata
from terrautils.metadata import clean_metadata


# CONNECTION SETTINGS
CLOWDER_HOST = "http://localhost:9000/"
CONN = Connector(None)

LOGFILE = open(os.path.join(OUTPUT_FOLDER, "build_log.txt"), "w+")
SENSOR_LIST = ["stereoTop"]
TIMESTAMP_FOLDER = True
DRY_RUN = False


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

def upload_ds(conn, host, key, sensor, date, timestamp, ds_files, ds_meta):
    if len(ds_files) > 0:
        year, month, date = date.split("-")
        if DRY_RUN:
            log("[%s] %s files" % (sensor+' - '+timestamp, len(ds_files)))
            return

        if TIMESTAMP_FOLDER:
            dataset_id = build_dataset_hierarchy(CONN, CLOWDER_HOST, CLOWDER_KEY, SPACE_ID,
                                                 sensor, year, month, date, sensor+' - '+timestamp)
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

                upload_ds(CONN, CLOWDER_HOST, CLOWDER_KEY, sensor, date, timestamp, DS_FILES, DS_META)

        # Otherwise the date is the dataset level
        elif os.path.isdir(DATE_DIR):
            log("Scanning datasets in %s" % SENSOR_DIR)
            for filename in os.listdir(DATE_DIR):
                if filename[0] != ".":
                    FILEPATH = os.path.join(TIMESTAMP_DIR, filename)
                    if filename.find("metadata.json") > -1:
                        DS_META = clean_metadata(loadJsonFile(FILEPATH), sensor)
                    else:
                        DS_FILES.append(FILEPATH)

            upload_ds(CONN, CLOWDER_HOST, CLOWDER_KEY, sensor, date, timestamp, DS_FILES, DS_META)

        # Don't create a dataset for metadata only

log("Completed.")
LOGFILE.close()

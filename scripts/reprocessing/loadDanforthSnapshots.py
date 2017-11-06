import sys, os, json
import logging

from pyclowder.connectors import Connector
from pyclowder.files import upload_metadata as upload_file_metadata
from pyclowder.datasets import upload_metadata as upload_dataset_metadata
from terrautils.extractors import get_collection_or_create, get_dataset_or_create, upload_to_dataset, build_metadata


"""
Given a root dir (experiment)...
- <dir/dirname_metadata.json> contains metadata that should be attached to each snapshot dataset
- <dir/SnapshotInfo.csv> has a row for each snapshot with additional metadata
    - <experiment> is dirname
    - <id> is snapshot ID without "snapshot" prefix
    - <plant barcode>
    - <car tag>
    - <timestamp>
    - <weight before>
    - <weight after>
    - <water amount>
    - <completed>
    - <measurement label>
    - <tag>
    - <tiles> is ;-separated list of files in snapshot directory without ".png" suffix

1. Load metadata from _metadata.json file
2. Create collection for this experiment in Clowder
3. Iterate over sub directories of dir
4. For each subdirectory...
    5. Get information from SnapshotInfo.csv for that snapshot
    6. Add #5 information temporarily to metadata from #1
    7. Create dataset in Clowder for this snapshot
    8. Add files & metadata to snapshot
    9. Submit dataset for extraction by PlantCV extractor

USAGE
    python loadDanforthSnapshots.py /home/clowder/sites/danforth/raw_data/TM015_F_051616
    python loadDanforthSnapshots.py /home/clowder/sites/danforth/raw_data/TM016_F_052716
"""

# Clowder connection info
clowder_host = "https://terraref.ncsa.illinois.edu/clowder/"
clowder_key = ""
# This is base Danforth space
root_space = "571fbfefe4b032ce83d96006"
# Upload as Danforth Site Clowder user
clowder_user = "terrarefglobus+danforth@ncsa.illinois.edu"
clowder_pass = ""
clowder_uid  = "5808d84864f4455cbe16f6d1"
# Set True to only print outputs without creating anything
dry_run = True


logging.basicConfig(filename='danforth_upload.log',level=logging.DEBUG)

def loadJsonFile(jsonfile):
    try:
        f = open(jsonfile)
        jsonobj = json.load(f)
        f.close()
        return jsonobj
    except IOError:
        print("- unable to open %s" % jsonfile)
        return {}

def getSnapshotDetails(csvfile, snapshotID):
    snap_file = open(csvfile, 'rU')
    snap_data = {}
    headers = snap_file.readline().rstrip('\n').replace(" ", "").split(",")

    # Table column order
    colnames = {}
    for i, col in enumerate(headers):
        colnames[col] = i

    for row in snap_file:
        entry = row.rstrip('\n').split(',')
        if entry[colnames['id']] != snapshotID:
            continue
        else:
            # Found row for this snapshot
            for colname in colnames:
                snap_data[colname] = entry[colnames[colname]]
            return snap_data

def parseDanforthBarcode(barcode):
    """Parses barcodes from the DDPSC phenotyping system.
    Args:
        barcode: barcode string
    Returns:
        parsed_barcode: barcode components
    Raises:
    """

    return {
        'species': barcode[0:2],
        'genotype': barcode[0:5],
        'treatment': barcode[5:7],
        'unique_id': barcode[7:]
    }

def formatImageMetadata(filename, experiment_md, snap_details):
    """Parses metadata from the DDPSC phenotyping system and returns metadata in JSON.
        For now there will be some manual reformatting of the metadata keywords.
    Args:
        filename: Image filename
        metadata: Experimental metadata
        data: List of metadata values
        fields: Dictionary of field names mapping to list IDs
    Returns:
        metadata_json: JSON-formatted metadata string
    Raises:
        StandardError: unrecognized camera type
        StandardError: unrecognized camera perspective
    """

    # Manual metadata reformatting (for now)
    # Format of side-view image names: imgtype_camera_rotation_zoom_lifter_gain_exposure_imageID
    # Format of top-view image names: imgtyp_camera_zoom_lifter_gain_exposure_imageID
    img_meta = filename.split('_')

    # Format camera_type
    if img_meta[0] == 'VIS':
        camera_type = 'visible/RGB'
    elif img_meta[0] == 'NIR':
        camera_type = 'near-infrared'
    # Format camera perspective
    if img_meta[1] == 'SV':
        perspective = 'side-view'
    elif img_meta[1] == 'TV':
        perspective = 'top-view'

    # SIDE VIEW
    if len(img_meta) == 8:
        rotation_angle = img_meta[2]
        zoom = (0.0008335 * int(img_meta[3].replace('z', ''))) + 0.9991665
        stage_position = img_meta[4].replace('h', '')
        camera_gain = img_meta[5].replace('g', '')
        camera_exposure = img_meta[6].replace('e', '')
        img_id = img_meta[7]
    # TOP VIEW
    elif len(img_meta) == 7:
        rotation_angle = 0
        zoom = (0.0008335 * int(img_meta[2].replace('z', ''))) + 0.9991665
        stage_position = img_meta[3].replace('h', '')
        camera_gain = img_meta[4].replace('g', '')
        camera_exposure = img_meta[5].replace('e', '')
        img_id = img_meta[6]

    # Extract human-readable values from Danforth Center barcodes
    barcode = parseDanforthBarcode(snap_details['barcode'])
    experiment_codes = experiment_md['sample']['barcode']
    if barcode['species'] in experiment_codes['species']:
        species = experiment_codes['species'][barcode['species']]
    if barcode['genotype'] in experiment_codes['genotypes']:
        genotype = experiment_codes['genotypes'][barcode['genotype']]
    if barcode['treatment'] in experiment_codes['treatments']:
        treatment = experiment_codes['treatments'][barcode['treatment']]

    return {
        'snapshot_id' : snap_details['id'],
        'plant_barcode' : snap_details['plantbarcode'],
        'camera_type' : camera_type,
        'perspective' : perspective,
        'rotation_angle' : rotation_angle,
        'zoom' : zoom,
        'imager_stage_vertical_position' : stage_position,
        'camera_gain' : camera_gain,
        'camera_exposure' : camera_exposure,
        'image_id' : img_id,
        'imagedate' : snap_details['timestamp'],
        'species' : species,
        'genotype' : genotype,
        'treatment' : treatment,
        'sample_id' : barcode['unique_id']
    }


conn = Connector({})
experiment_root = sys.argv[1]
experiment_name = os.path.basename(experiment_root)
if os.path.exists(experiment_root):
    logging.debug("Searching for index files in %s" % experiment_root)
    md_file  = os.path.join(experiment_root, experiment_name+"_metadata.json")
    csv_file = os.path.join(experiment_root, "SnapshotInfo.csv")

    if not os.path.isfile(md_file):
        logging.debug("No metadata.json file found in %s" % experiment_root)
        sys.exit(1)
    if not os.path.isfile(md_file):
        logging.debug("No SnapshotInfo.csv file found in %s" % experiment_root)
        sys.exit(1)

    logging.debug("Found index files; loading %s" % md_file)
    base_md = loadJsonFile(md_file)
    experiment_md = {
        "sensor": "ddpscIndoorSuite",
        "date": base_md['experiment']['planting_date'],
        "metadata": base_md,
        # These two will be fetched from CSV file
        "timestamp": None,
        "snapshot": None
    }

    # Create Clowder collection
    if not dry_run:
        experiment_coll = get_collection_or_create(clowder_host, clowder_key, clowder_user, clowder_pass, experiment_name,
                                                   parent_space=root_space)
        logging.debug("Created collection %s [%s]" % (experiment_name, experiment_coll))
    else:
        logging.debug("Skipping collection %s [%s]" % (experiment_name, "DRY RUN"))
        logging.debug("Metadata for %s: %s" % (experiment_name, experiment_md))

    for snap_dir in os.path.listdir(experiment_root):
        if os.path.isdir(snap_dir):
            logging.debug("Scanning files in %s" % snap_dir)
            snap_id = snap_dir.replace("snapshot", "")
            snap_details = getSnapshotDetails(csv_file, snap_id)

            # Create Clowder dataset and add metadata
            snap_md = {
                "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
                "content": base_md['experiment'],
                "agent": {
                    "@type": "cat:user",
                    "user_id": "https://terraref.ncsa.illinois.edu/clowder/api/users/%s" % clowder_uid
                }
            }
            if not dry_run:
                snap_dataset = get_dataset_or_create(clowder_host, clowder_key, clowder_user, clowder_pass, snap_dir,
                                                     experiment_coll, root_space)
                logging.debug("Created collection %s [%s]" % (snap_dir, snap_dataset))
                snap_md["dataset_id"] = snap_dataset
                upload_dataset_metadata(conn, clowder_host, clowder_key, snap_dataset, snap_md)
                logging.debug("Uploaded metadata to [%s]" % snap_dataset)
            else:
                logging.debug("Skipping dataset %s [%s]" % (snap_dir, "DRY RUN"))
                logging.debug("Details for %s: %s" % (snap_dir, snap_details))
                logging.debug("Metadata for %s: %s" % (snap_dir, snap_md))

            # Upload files and metadata to Clowder
            snap_files = os.path.listdir(os.path.join(experiment_root, snap_dir))
            for img_file in snap_files:
                img_path = os.path.join(experiment_root, snap_dir, img_file)
                img_md = formatImageMetadata(img_file, experiment_md, snap_details)
                file_md = {
                    "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
                    "content": img_md,
                    "agent": {
                        "@type": "cat:user",
                        "user_id": "https://terraref.ncsa.illinois.edu/clowder/api/users/%s" % clowder_uid
                    }
                }
                if not dry_run:
                    file_id = upload_to_dataset(conn, clowder_host, clowder_user, clowder_pass, snap_dataset, img_path)
                    logging.debug("Created file %s [%s]" % (img_file, file_id))
                    file_md["file_id"] = file_id
                    upload_file_metadata(conn, clowder_host, clowder_key, file_id, file_md)
                    logging.debug("Uploaded metadata to [%s]" % file_id)
                else:
                    logging.debug("Skipping file %s [%s]" % (img_file, "DRY RUN"))
                    logging.debug("Metadata for %s: %s" % (img_file, file_md))

    logging.debug("Experiment uploading complete.")

else:
    logging.debug("%s does not exist" % experiment_root)
    sys.exit(1)
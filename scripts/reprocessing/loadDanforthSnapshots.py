import sys, os, json
import requests
from urllib3.filepost import encode_multipart_formdata

###
# RUN AS USER UBUNTU
###

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
"""


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


experiment_root = sys.argv[1]
experiment_name = os.path.basename(experiment_root)
if os.path.exists(experiment_root):
    md_file  = os.path.join(experiment_root, experiment_name+"_metadata.json")
    csv_file = os.path.join(experiment_root, "SnapshotInfo.csv")

    if not os.path.isfile(md_file):
        print("No metadata.json file found in %s" % experiment_root)
        sys.exit(1)
    if not os.path.isfile(md_file):
        print("No SnapshotInfo.csv file found in %s" % experiment_root)
        sys.exit(1)

    base_md = loadJsonFile(md_file)
    experiment_md = {
        "sensor": "ddpscIndoorSuite",
        "date": base_md['experiment']['planting_date'],
        "metadata": base_md,
        # These two will be fetched from CSV file
        "timestamp": None,
        "snapshot": None
    }

    # TODO: Create Clowder collection

    for snap_dir in os.path.listdir(experiment_root):
        if os.path.isdir(snap_dir):
            snap_id = snap_dir.replace("snapshot", "")
            snap_details = getSnapshotDetails(csv_file, snap_id)

            # TODO: Create Clowder dataset

            snap_files = os.path.listdir(os.path.join(experiment_root, snap_dir))
            for img_file in snap_files:
                img_md = formatImageMetadata(img_file, experiment_md, snap_details)

                # TODO: Upload file + metadata to Clowder




def submitGroupToClowder(group):
    """Create collection/dataset if needed and post files/metadata to it"""
    c_sensor = group['sensor']
    c_date = c_sensor + " - "+ group['date']
    c_year = c_sensor + " - "+ group['date'].split('-')[0]
    c_month = c_year + "-" + group['date'].split('-')[1]

    # Space is organized per-site, will just hardcode these for now
    if c_sensor == "ddpscIndoorSuite":
        c_space = "571fbfefe4b032ce83d96006"
        c_user = "terrarefglobus+danforth@ncsa.illinois.edu"
        c_user_id = "5808d84864f4455cbe16f6d1"
        c_pass = ""
        c_context = "https://terraref.ncsa.illinois.edu/metadata/danforth#"
    else:
        c_space = "571fb3e1e4b032ce83d95ecf"
        c_user = "terrarefglobus+uamac@ncsa.illinois.edu"
        c_user_id = "57adcb81c0a7465986583df1"
        c_pass = ""
        c_context = "https://terraref.ncsa.illinois.edu/metadata/uamac#"

    sess = requests.Session()
    sess.auth = (c_user, c_pass)

    print(c_sensor +" | "+c_year +" | "+c_month +" | "+c_date)

    id_sensor = fetchCollectionByName(c_sensor, c_space, sess)
    id_year = fetchCollectionByName(c_year, c_space, sess)
    id_month = fetchCollectionByName(c_month, c_space, sess)
    # Nest new collections if necessary
    if id_year['created']: associateChildCollection(id_sensor['id'], id_year['id'], sess)
    if id_month['created']: associateChildCollection(id_year['id'], id_month['id'], sess)

    if group['snapshot'] is not None:
        # Danforth uses Snapshot as dataset
        c_dataset = c_sensor + " - " + group['snapshot']
        id_date = fetchCollectionByName(c_date, c_space, sess)
        if id_date["created"]: associateChildCollection(id_month['id'], id_date['id'], sess)
        id_dataset = fetchDatasetByName(c_dataset, c_space, id_sensor["id"], id_year["id"], id_month["id"], id_date["id"], sess)
    elif group['timestamp'] is None:
        # Some have the date level as the dataset, not a collection
        c_dataset = c_sensor + " - " + group['date']
        id_dataset = fetchDatasetByName(c_dataset, c_space, id_sensor["id"], id_year["id"], id_month["id"], None, sess)
    else:
        c_dataset = c_sensor + " - " + group['timestamp']
        id_date = fetchCollectionByName(c_date, c_space, sess)
        if id_date["created"]: associateChildCollection(id_month['id'], id_date['id'], sess)
        id_dataset = fetchDatasetByName(c_dataset, c_space, id_sensor["id"], id_year["id"], id_month["id"], id_date["id"], sess)

    # Perform actual posts
    if id_dataset:
        if group['metadata']:
            md = {
                "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld",
                             {"@vocab": c_context}],
                "content": group['metadata'],
                "agent": {
                    "@type": "cat:user",
                    "user_id": "https://terraref.ncsa.illinois.edu/clowder/api/users/%s" % c_user_id
                }
            }
            sess.post(clowderURL+"/api/datasets/"+id_dataset+"/metadata.jsonld",
                      headers={'Content-Type':'application/json'},
                      data=json.dumps(md))
            print("++++ added metadata to %s (%s)" % (c_dataset, id_dataset))

        fileFormData = []
        for f in group['files']:
            # METADATA
            # Use [1,-1] to avoid json.dumps wrapping quotes
            # Replace \" with " to avoid json.dumps escaping quotes
            fmd = group['file_md'][f] if f in group['file_md'] else None
            mdstr = ', "md":'+json.dumps(fmd).replace('\\"', '"') if fmd else ""
            if f.find("/gpfs/largeblockFS/") > -1:
                f = f.replace("/gpfs/largeblockFS/projects/arpae/terraref/", "/home/clowder/")

            fileFormData.append(("file",'{"path":"%s"%s}' % (f, mdstr)))

        if len(fileFormData) > 0:
            (content, header) = encode_multipart_formdata(fileFormData)
            fi = sess.post(clowderURL+"/api/uploadToDataset/"+id_dataset,
                                       headers={'Content-Type':header},
                                       data=content)

            if fi.status_code == 200:
                print("++++ added files to %s (%s)" % (c_dataset, id_dataset))
            else:
                print(fi.status_code)
                print(fi.status_message)

inputfile = sys.argv[1]
danforthCSV = "/home/clowder/sites/danforth/raw_data/sorghum_pilot_dataset/SnapshotInfo.csv"
clowderURL = "https://terraref.ncsa.illinois.edu/clowder"
lastLine = ""

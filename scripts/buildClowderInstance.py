import sys, os, json
import requests
import psycopg2
from urllib3.filepost import encode_multipart_formdata

###
# RUN AS USER UBUNTU
###

# TODO: Update pipeline to use new nested dataset structure

def main():
    with open(inputfile, 'r') as inp:
        curr_group = {
            "sensor": None,         # e.g. "co2Sensor"
            "date": None,           # e.g. "2016-08-02"
            "timestamp": None,      # e.g. "2016-08-02__09-42-51-195"
            "metadata": None,       # e.g. {"md_field": "value", "md_field2": 100}
            "snapshot": None        # e.g. "snapshot299661", used for Danforth data
        }
        curr_group_files = []
        curr_group_file_metadata = {}

        lastLineFound = True if lastLine == "" else False
        for line in inp:
            full_path = line.rstrip()
            file_metadata = None
            if not lastLineFound:
                if lastLine == full_path:
                    print("Found last line; resuming uploads.")
                    lastLineFound = True
            else:
                full_path = full_path.replace("/gpfs/largeblockFS/projects/arpae/terraref/", "/home/clowder/")
                if full_path == "": continue
                curr_info = {"sensor": None, "date": None, "timestamp": None, "metadata": None, "snapshot": None}

                # RAW_DATA
                if full_path.find("raw_data") > -1:
                    # Extract metadata properties from json if found
                    if full_path.endswith("metadata.json"):
                        if full_path.find("/danforth/") > -1:
                            curr_info = getDanforthInfoFromJson(full_path)
                            if curr_group['sensor'] == None:
                                curr_group = curr_info
                        else:
                            curr_info = getGantryInfoFromPath(full_path)
                            curr_info['metadata'] = getGantryMetadata(full_path)
                            if curr_group['sensor'] == None:
                                curr_group = curr_info
                    # For other files we just need to compare if we're looking at same dataset
                    else:
                        if full_path.find("/danforth/") > -1:
                            (curr_info['snapshot'], file_metadata) = getDanforthSnapshotFromPath(full_path)
                        else:
                            curr_info = getGantryInfoFromPath(full_path)
                # LEVEL_1 DATA
                if full_path.find("Level_1") > -1:
                    if full_path.find("ddpscIndoorSuite") > -1:
                        timeinfo = full_path.split("/")[-2].replace("ddpscIndoorSuite - ", "")
                        date = timeinfo.split("__")[0]
                        curr_info = {
                            "sensor": "ddpscIndoorSuite",
                            "date": date,
                            "timestamp": timeinfo,
                            "snapshot": timeinfo,
                            "metadata": {
                                "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
                                "content": {"status": "COMPLETED"},
                                "agent": {
                                    "@type": "cat:extractor",
                                    "extractor_id": clowderURL + "/api/extractors/terra.plantcv"
                                }
                            }
                        }
                    elif full_path.find("demosaic"):
                        # /projects/arpae/terraref/sites/ua-mac/Level_1/demosaic/2016-11-18/2016-11-18__08-48-36-847/0b8908ca-4c1a-44aa-a72e-e5ec0aa3ec4d_left.jpg
                        timeinfo = full_path.split("/")[-2]
                        date = timeinfo.split("__")[0]
                        curr_info = {
                            "sensor": "stereoTop",
                            "date": date,
                            "timestamp": timeinfo,
                            "metadata": {
                                "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
                                "content": {"status": "COMPLETED"},
                                "agent": {
                                    "@type": "cat:extractor",
                                    "extractor_id": clowderURL + "/api/extractors/terra.demosaic"
                                }
                            }
                        }
                    elif full_path.find("EnvironmentLogger"):
                        # /projects/arpae/terraref/sites/ua-mac/Level_1/EnvironmentLogger/2016-09-21/2016-09-21_00-27-10_environmentlogger.nc
                        date = full_path.split("/")[-2]
                        curr_info = {
                            "sensor": "EnvironmentLogger",
                            "date": date,
                            "timestamp": None,
                            "metadata": None
                        }
                    elif full_path.find("scanner3DTop"):
                        # /projects/arpae/terraref/sites/ua-mac/Level_1/scanner3DTop/2016-12-01/2016-12-01__11-30-45-448/4880b4f9-abd5-4a19-b38e-5308c2926d1e__Top-heading-east_0.ply
                        timeinfo = full_path.split("/")[-2]
                        date = timeinfo.split("__")[0]
                        curr_info = {
                            "sensor": "scanner3DTop",
                            "date": date,
                            "timestamp": timeinfo,
                            "metadata": None
                        }

                # If the properties don't match, submit this group and start a new group
                submit = False
                if full_path.find("/danforth/") > -1:
                    if curr_info['snapshot'] != curr_group['snapshot']:
                        submit = True
                    elif curr_info['metadata'] is not None:
                        curr_group['metadata'] = curr_info['metadata']
                else:
                    if (curr_info["sensor"] != curr_group["sensor"] or
                            curr_info["date"] != curr_group["date"] or
                            curr_info["timestamp"] != curr_group["timestamp"]):
                        submit = True
                    elif curr_info['metadata'] is not None:
                        curr_group['metadata'] = curr_info['metadata']

                # We have reached a new dataset, so submit the current one as a batch before continuing
                if submit:
                    if curr_group['sensor'] is not None:
                        curr_group['files'] = curr_group_files
                        submitGroupToClowder(curr_group)
                    curr_group = curr_info
                    curr_group_files = []
                if not full_path.endswith("metadata.json"):
                    curr_group_files.append(full_path)
                    if file_metadata:
                        curr_group_file_metadata[full_path] = file_metadata

        # Finally handle any leftovers
        curr_group['files'] = curr_group_files
        curr_group['file_md'] = curr_group_file_metadata
        submitGroupToClowder(curr_group)

def connectToPostgres():
    """
    If globusmonitor database does not exist yet:
        $ cd /usr/lib/postgresql/9.5/bin/
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

    print("Connected to Postgres")
    return conn

def loadJsonFile(jsonfile):
    try:
        f = open(jsonfile)
        jsonobj = json.load(f)
        f.close()
        return jsonobj
    except IOError:
        print("- unable to open %s" % jsonfile)
        return {}

# Danforth Center barcode parser
def barcode_parser(barcode):
    """Parses barcodes from the DDPSC phenotyping system.
    Args:
        barcode: barcode string
    Returns:
        parsed_barcode: barcode components
    Raises:
    """

    parsed_barcode = {}
    parsed_barcode['species'] = barcode[0:2]
    parsed_barcode['genotype'] = barcode[0:5]
    parsed_barcode['treatment'] = barcode[5:7]
    parsed_barcode['unique_id'] = barcode[7:]
    return parsed_barcode

# Danforth Center image metadata formatter
def metadata_to_json(filename, metadata, data, fields):
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

    if len(img_meta) == 8:
        # Is this a side-view image?
        rotation_angle = img_meta[2]
        zoom = (0.0008335 * int(img_meta[3].replace('z', ''))) + 0.9991665
        stage_position = img_meta[4].replace('h', '')
        camera_gain = img_meta[5].replace('g', '')
        camera_exposure = img_meta[6].replace('e', '')
        img_id = img_meta[7]
    elif len(img_meta) == 7:
        # Is this a top-view image?
        rotation_angle = 0
        zoom = (0.0008335 * int(img_meta[2].replace('z', ''))) + 0.9991665
        stage_position = img_meta[3].replace('h', '')
        camera_gain = img_meta[4].replace('g', '')
        camera_exposure = img_meta[5].replace('e', '')
        img_id = img_meta[6]

    # Extract metadata from Danforth Center barcodes
    parsed_barcode = barcode_parser(data[fields['plant barcode']])
    if parsed_barcode['species'] in metadata['sample']['barcode']['species']:
        species = metadata['sample']['barcode']['species'][parsed_barcode['species']]

    if parsed_barcode['genotype'] in metadata['sample']['barcode']['genotypes']:
        genotype = metadata['sample']['barcode']['genotypes'][parsed_barcode['genotype']]

    if parsed_barcode['treatment'] in metadata['sample']['barcode']['treatments']:
        treatment = metadata['sample']['barcode']['treatments'][parsed_barcode['treatment']]

    file_metadata = {'snapshot_id' : data[fields['id']], 'plant_barcode' : data[fields['plantbarcode']],
                     'camera_type' : camera_type, 'perspective' : perspective, 'rotation_angle' : rotation_angle,
                     'zoom' : zoom, 'imager_stage_vertical_position' : stage_position, 'camera_gain' : camera_gain,
                     'camera_exposure' : camera_exposure, 'image_id' : img_id, 'imagedate' : data[fields['timestamp']],
                     'species' : species, 'genotype' : genotype, 'treatment' : treatment,
                     'sample_id' : parsed_barcode['unique_id']}

    #metadata_json = json.dumps(file_metadata)
    #return metadata_json
    return file_metadata

def getImageMdFromCSV(snapshotID, imagename):
    csvfile = open(danforthCSV, 'rU')
    header = csvfile.readline().rstrip('\n').replace(" ", "")

    exp_metadata = {
        "experiment" : {
            "planting_date": "2014-05-27",
            "title": "Sorghum Pilot Experiment - Danforth Center Phenotyping Facility - 2014-05-27",
            "author": "Noah Fahlgren",
            "project": "TERRA-REF",
            "location": "Donald Danforth Plant Science Center",
            "instrument": "Bellwether Phenotyping Facility",
            "growth_medium": "MetroMix360 potting mix with 14-14-14 Osmocote"
        },
        "sample" : {
            "barcode" : {
                "format": {
                    "species": "0-1",
                    "genotype": "0-4",
                    "treatment": "5-6",
                    "unique_id": "7-12"
                },
                "species": {
                    "Fp": "Sorghum bicolor",
                    "Fa": "Sorghum bicolor",
                    "Fr": "Sorghum bicolor"
                },
                "genotypes": {
                    "Fr001": "BTx623",
                    "Fp001": "BTx642",
                    "Fp002": "Tx7000",
                    "Fa001": "Tx430"
                },
                "treatments": {
                    "AA": "100%: 217 ml water (47.6% VWC)",
                    "AB": "80%: 173.6 ml water (37.5% VWC)",
                    "AC": "60%: 130.2 ml water (27.3% VWC)",
                    "AD": "40%: 86.8 ml water (17.2% VWC)"
                }
            }
        }
    }

    # Table column order
    cols = header.split(',')
    colnames = {}
    for i, col in enumerate(cols):
        colnames[col] = i

    for row in csvfile:
        data = row.rstrip('\n').split(',')

        if data[colnames['id']] != snapshotID:
            continue
        else:
            # The tiles column has the list of image names for each snapshot - remove last item b/c is trailing semicolon
            img_list = data[colnames['tiles']][:-1]
            imgs = img_list.split(';')

            for img in imgs:
                image_name = img + '.png'
                img_metadata = metadata_to_json(img, exp_metadata, data, colnames)

def getDanforthInfoFromJson(jsonpath):
    """
    Load dataset properties & metadata from json file.
    
    Example contents:
    {"author": "Noah Fahlgren", "growth_medium": "MetroMix360 potting mix with 14-14-14 Osmocote",
     "title": "Sorghum Pilot Experiment - Danforth Center Phenotyping Facility - 2014-05-27",
     "project": "TERRA-REF", "instrument": "Bellwether Phenotyping Facility",
     "location": "Donald Danforth Plant Science Center",
     "snapshot_id": "299661", "planting_date": "2014-05-27"}
    """
    jsonobj = loadJsonFile(jsonpath)

    if 'experiment' in jsonobj:
        if 'planting_date' in jsonobj['experiment']:
            j_date = jsonobj['experiment']['planting_date']
        elif "title" in jsonobj:
            j_date = jsonobj['experiment']['title'].split(" - ")[2]
        else:
            j_date = None
    else:
        if 'planting_date' in jsonobj:
            j_date = jsonobj['planting_date']
        elif "title" in jsonobj:
            j_date = jsonobj['title'].split(" - ")[2]
        else:
            j_date = None

    return {"sensor": "ddpscIndoorSuite",
            "date": j_date,
            "timestamp": None,
            "snapshot": getDanforthSnapshotFromPath(jsonpath),
            "metadata": jsonobj
    }

def getDanforthSnapshotFromPath(filepath):
    """Get snapshot info from path."""
    parts = filepath.split("/")
    if parts[-2].find("snapshot") > -1:
        # Get other plant information from SnapshotInfo.csv
        img_metadata = getImageMdFromCSV(parts[-2].replace('snapshot',''), os.path.basename(filepath).replace('.png', ''))
        return (parts[-2], img_metadata)
    else:
        return None

def getGantryMetadata(jsonpath):
    """Load metadata from json file."""
    return loadJsonFile(jsonpath)

def getGantryInfoFromPath(filepath):
    """Get dataset info from path."""
    parts = filepath.split("/")
    # First check whether immediate parent folder is a timestamp or a date
    if parts[-2].find("__") > -1:
        # raw_data/scanner3DTop/2016-01-01/2016-08-02__09-42-51-195/file.json
        j_timestamp = parts[-2]
        j_date = parts[-3]
        j_sensor = parts[-4]
    else:
        # raw_data/EnvironmentLogger/2016-01-01/2016-08-03_04-05-34_environmentlogger.json
        # raw_data/weather/2016-08-30/WeatherStation_SecData_2016_08_30_0720.dat
        j_timestamp = None
        j_date = parts[-2]
        j_sensor = parts[-3]

    if j_sensor == "weather":
        j_sensor = "Weather Station"

    return {
        "sensor": j_sensor,
        "date": j_date,
        "timestamp": j_timestamp,
        "snapshot": None,
        "metadata": None
    }

"""Find dataset id if dataset exists, creating if necessary"""
def fetchDatasetByName(datasetName, parentSpace, sensorId, yearId, monthId, dateId, requestsSession):
    if datasetName not in datasetMap:
        if dateId:
            dataObj = '{"name": "%s", "collection": ["%s"], "space": ["%s"]}' % (
                datasetName, dateId, parentSpace)
        else:
            dataObj = '{"name": "%s", "collection": ["%s"], "space": ["%s"]}' % (
                datasetName, monthId, parentSpace)

        ds = requestsSession.post(clowderURL+"/api/datasets/createempty",
                                  headers={"Content-Type": "application/json"},
                                  data=dataObj)

        if ds.status_code == 200:
            dsid = ds.json()['id']
            datasetMap[datasetName] = dsid
            writeDatasetRecordToDatabase(datasetName, dsid)
            print("+ created dataset '%s' (%s)" % (datasetName, dsid))
            return dsid
        else:
            print("ERROR - cannot create dataset (%s: %s)" % (ds.status_code, ds.text))
            return None

    else:
        # We have a record of it, but check that it still exists before returning the ID
        dsid = datasetMap[datasetName]
        return dsid

"""Find dataset id if dataset exists, creating if necessary"""
def fetchCollectionByName(collectionName, parentSpace, requestsSession):
    if collectionName not in collectionMap:
        coll = requestsSession.post(clowderURL+"/api/collections",
                                    headers={"Content-Type": "application/json"},
                                    data='{"name": "%s", "description": ""}' % collectionName)

        if coll.status_code == 200:
            collid = coll.json()['id']
            collectionMap[collectionName] = collid
            writeCollectionRecordToDatabase(collectionName, collid)
            print("+ created collection '%s' (%s)" % (collectionName, collid))
            # Add new collection to primary space if defined
            requestsSession.post(clowderURL+"/api/spaces/%s/addCollectionToSpace/%s" %
                                     (parentSpace, collid))
            return {
                "id": collid,
                "created": True
            }
        else:
            print("ERROR - cannot create collection (%s: %s)" % (coll.status_code, coll.text))
            return None
    else:
        return {
                "id": collectionMap[collectionName],
                "created": False
            }

def associateChildCollection(parentId, childId, requestsSession):
    requestsSession.post(clowderURL+"/api/collections/%s/addSubCollection/%s" % (parentId, childId))

"""Write dataset (name -> clowder_id) mapping to PostgreSQL database"""
def writeDatasetRecordToDatabase(dataset_name, dataset_id):

    q_insert = "INSERT INTO datasets (name, clowder_id) VALUES (%s, %s) " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id=%s;"

    curs = psql_conn.cursor()
    curs.execute(q_insert, (dataset_name, dataset_id, dataset_id))
    psql_conn.commit()
    curs.close()

"""Write collection (name -> clowder_id) mapping to PostgreSQL database"""
def writeCollectionRecordToDatabase(collection_name, collection_id):
    q_insert = "INSERT INTO collections (name, clowder_id) VALUES (%s, %s) " \
               "ON CONFLICT (name) DO UPDATE SET clowder_id=%s;"

    curs = psql_conn.cursor()
    curs.execute(q_insert, (collection_name, collection_id, collection_id))
    psql_conn.commit()
    curs.close()

def readRecordsFromDatabase():
    q_fetch_datas = "SELECT * FROM datasets;"
    q_detch_colls = "SELECT * FROM collections;"

    curs = psql_conn.cursor()
    print("...fetching dataset mappings from PostgreSQL...")
    curs.execute(q_fetch_datas)
    for currds in curs:
        datasetMap[currds[0]] = currds[1]

    print("...fetching collection mappings from PostgreSQL...")
    curs.execute(q_detch_colls)
    for currco in curs:
        collectionMap[currco[0]] = currco[1]
    curs.close()

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
            #mdstr = ', "md":' + json.dumps(fobj['md'])[1:-1].replace('\\"',
            fmd = group['file_md'][f] if f in group['file_md'] else None
            mdstr = ', "md":'+json.dumps(fmd)[1:-1].replace('\\"', '"') if fmd else ""
            if f.find("/gpfs/largeblockFS/") > -1:
                f = f.replace("/gpfs/largeblockFS/projects/arpae/terraref/", "/home/clowder/")

            fileFormData.append(("file",'{"path":"%s"%s}' % (f, mdstr)))

        if len(fileFormData) > 0:
            (content, header) = encode_multipart_formdata(fileFormData)
            fi = sess.post(clowderURL+"/api/uploadToDataset/"+id_dataset,
                                       headers={'Content-Type':header},
                                       data=content)
        print("++++ added files to %s (%s)" % (c_dataset, id_dataset))

inputfile = sys.argv[1]
danforthCSV = "/home/clowder/sites/danforth/raw_data/sorghum_pilot_dataset/SnapshotInfo.csv"
clowderURL = "https://terraref.ncsa.illinois.edu/clowder"
lastLine = ""

# Dictionaries that map Clowder name -> Clowder ID
collectionMap = {}
datasetMap = {}

psql_conn = connectToPostgres()
readRecordsFromDatabase()

if __name__ == '__main__':
    main()

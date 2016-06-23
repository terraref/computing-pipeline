#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import argparse
import requests
import posixpath
import json


# Parse command-line arguments
###########################################
def options():
    """Parse command line options.

    Args:

    Returns:
        argparse object

    Raises:
        IOError: if dir does not exist.
        IOError: if the metadata file SnapshotInfo.csv does not exist in dir.
        IOError: if the experimental metadata file does not exist.
    """

    parser = argparse.ArgumentParser(description="PlantCV dataset Clowder uploader.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", help="Input directory containing image snapshots.", required=True)
    parser.add_argument("-c", "--collection",
                        help="Clowder collection name. This is a container for all the uploaded datasets/snapshots",
                        required=True)
    parser.add_argument("-u", "--url", help="Clowder URL.", required=True)
    parser.add_argument("-U", "--username", help="Clowder username.", required=True)
    parser.add_argument("-p", "--password", help="Clowder password.", required=True)
    parser.add_argument("-m", "--meta", help="Experiment metadata file in JSON format", required=True)
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-n", "--dryrun", help="Dry run, do not upload files.", action="store_true")

    args = parser.parse_args()

    if not os.path.exists(args.dir):
        raise IOError("Directory does not exist: {0}".format(args.dir))
    if not os.path.exists(args.dir + '/SnapshotInfo.csv'):
        raise IOError("The snapshot metadata file SnapshotInfo.csv does not exist in {0}".format(args.dir))
    if not os.path.exists(args.meta):
        raise IOError("The metadata file {0} does not exist".format(args.meta))

    return args


###########################################

# Main
###########################################
def main():
    """Main program.
    """

    # Get options
    args = options()

    # Create new session
    sess = requests.Session()
    sess.auth = (args.username, args.password)

    # Create a new collection
    coll_id = create_clowder_collection(sess, args.url, args.collection, args.dryrun)

    # Read experiment metadata from JSON
    meta_json = open(args.meta, 'rU')
    exp_metadata = json.load(meta_json)

    # Open the SnapshotInfo.csv file
    csvfile = open(posixpath.join(args.dir, 'SnapshotInfo.csv'), 'rU')

    # Read the first header line
    header = csvfile.readline()
    header = header.rstrip('\n')

    # Remove whitespace from the field names
    header = header.replace(" ", "")

    # Table column order
    cols = header.split(',')
    colnames = {}
    for i, col in enumerate(cols):
        colnames[col] = i

    # Read through the CSV file to identify datasets/snapshots
    dataset_count = 0
    for row in csvfile:
        # Remove newline
        row = row.rstrip('\n')
        # Create a list of data columns by splitting on commas
        data = row.split(',')
        # The tiles column has the list of image names for each snapshot
        img_list = data[colnames['tiles']]
        # Remove the trailing semicolon from the tiles list
        img_list = img_list[:-1]
        # Does this snapshot contain images?
        if len(img_list) > 0:
            # Name the dataset after the snapshot ID
            dataset_name = 'snapshot' + data[colnames['id']]
            ds_id = create_clowder_dataset(sess, args.url, dataset_name,
                                           coll_id, exp_metadata['experiment'], args.dryrun)

            # Create list of images by splitting the tiles column on semicolons
            imgs = img_list.split(';')
            # Add each image file to the Clowder dataset
            for img in imgs:
                # Images are in PNG format
                image_name = img + '.png'
                # Format image file metadata
                img_metadata = metadata_to_json(img, exp_metadata, data, colnames)
                # Build image file path
                image_path = posixpath.join(args.dir, 'snapshot' + data[colnames['id']], image_name)
                upload_file_to_clowder(sess, args.url, image_path, ds_id, {image_name : json.dumps(img_metadata)}, args.dryrun)

            dataset_count += 1
            if args.verbose:
                if dataset_count % 100 == 0:
                    print('Datasets uploaded: ' + str(dataset_count), file=sys.stderr)

###########################################


# Create Clowder collection
###########################################
def create_clowder_collection(session, url, collection, dryrun=False):
    """Create a Clowder collection.

    Args:
        session: http session
        url: Clowder URL
        collection: Clowder collection name
        dryrun: Boolean. If true, no POST requests are made
    Returns:
        coll_id: Clowder collection ID

    Raises:
        StandardError: HTTP POST return not 200
    """

    if dryrun is False:
        # Create a new collection
        coll_r = session.post(posixpath.join(url, "api/collections"), headers={"Content-Type": "application/json"},
                              data='{"name": "' + str(collection) + '"}')

    # Get collection ID
    if dryrun:
        coll_id = 'testcollection'
    else:
        # Was the collection created successfully?
        if coll_r.status_code == 200:
            # Collection ID
            coll_id = coll_r.json()["id"]
        else:
            raise StandardError("Creating collection failed: Return value = {0}".format(coll_r.status_code))

    return coll_id
###########################################


# Create Clowder dataset
###########################################
def create_clowder_dataset(session, url, dataset, collection_id, metadata, dryrun=False):
    """Create a Clowder dataset and associate it with a collection.

    Args:
        session: http session
        url: Clowder URL
        dataset: Clowder dataset name
        collection_id: Clowder collection ID
        metadata: Experimental metadata to tag with Clowder dataset
        dryrun: Boolean. If true, no POST requests are made
    Returns:
        ds_id: Clowder dataset ID

    Raises:
        StandardError: HTTP POST return not 200
    """

    if dryrun is False:
        # Create a new dataset
        ds_r = session.post(posixpath.join(url, "api/datasets/createempty"),
                            headers={"Content-Type": "application/json"}, data='{"name": "' + str(dataset) + '"}')

    # Get dataset ID
    if dryrun:
        ds_id = 'testdataset'
    else:
        # Was the dataset created successfully?
        if ds_r.status_code == 200:
            # Dataset ID
            ds_id = ds_r.json()["id"]
        else:
            raise StandardError("Creating dataset failed: Return value = {0}".format(ds_r.status_code))

    # Add dataset to existing collection
    if dryrun is False:
        coll_r = session.post(posixpath.join(url, "api/collections", collection_id, "datasets", ds_id))

        # Was the dataset added to the collection successfully?
        if coll_r.status_code != 200:
            raise StandardError("Adding dataset {0} to collection {1} failed: Return value = {2}".format(
                ds_id, collection_id, coll_r.status_code))

    # Add metadata to dataset
    if dryrun is False:
        meta_r = session.post(posixpath.join(url, "api/datasets", ds_id, "metadata"),
                              headers={"Content-Type": "application/json"}, data=json.dumps(metadata))

        # Was the metadata added to the dataset successfully?
        if meta_r.status_code != 200:
            raise StandardError("Adding metadata to dataset {0} failed: Return value = {1}".format(ds_id,
                                                                                                   meta_r.status_code))

    return ds_id
###########################################


# Upload file to a Clowder dataset
###########################################
def upload_file_to_clowder(session, url, file, dataset_id, metadata, dryrun=False):
    """Upload a file to a Clowder dataset.

    Args:
        session: http session
        url: Clowder URL
        file: File name and path
        dataset_id: Clowder dataset ID
        dryrun: Boolean. If true, no POST requests are made
    Returns:

    Raises:
        StandardError: HTTP POST return not 200
    """

    # Make sure file exists
    if os.path.exists(file):
        # Upload image file
        if dryrun is False:
            # Open file in binary mode
            f = open(file, 'rb')
            # Upload file to Clowder
            up_r = session.post(posixpath.join(url, "api/uploadToDataset", dataset_id),
                                files={"File" : f}, data=metadata)

            # Was the upload successful?
            if up_r.status_code != 200:
                raise StandardError("Uploading file failed: Return value = {0}".format(up_r.status_code))
    else:
        print("ERROR: Image file {0} does not exist".format(file), file=sys.stderr)
###########################################


# Danforth Center barcode parser
###########################################
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

###########################################


# Danforth Center image metadata formatter
###########################################
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
    else:
        raise StandardError("Unrecognized camera type {0} for image {1}.".format(img_meta[0], filename))

    # Format camera perspective
    if img_meta[1] == 'SV':
        perspective = 'side-view'
    elif img_meta[1] == 'TV':
        perspective = 'top-view'
    else:
        raise StandardError("Unrecognized camera perspective {0} for image {1}.".format(img_meta[1], filename))

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
    else:
        raise StandardError("Unrecognized image name format for image {0}".format(filename))

    # Extract metadata from Danforth Center barcodes
    parsed_barcode = barcode_parser(data[fields['plantbarcode']])
    if parsed_barcode['species'] in metadata['sample']['barcode']['species']:
        species = metadata['sample']['barcode']['species'][parsed_barcode['species']]
    else:
        raise StandardError("Unrecognized species code {0} for image {1}".format(parsed_barcode['species'], filename))

    if parsed_barcode['genotype'] in metadata['sample']['barcode']['genotypes']:
        genotype = metadata['sample']['barcode']['genotypes'][parsed_barcode['genotype']]
    else:
        raise StandardError("Unrecognized genotype code {0} for image {1}".format(parsed_barcode['genotype'], filename))

    if parsed_barcode['treatment'] in metadata['sample']['barcode']['treatments']:
        treatment = metadata['sample']['barcode']['treatments'][parsed_barcode['treatment']]
    else:
        raise StandardError("Unrecognized treatment code {0} for image {1}".format(parsed_barcode['treatment'],
                                                                                   filename))

    file_metadata = {'snapshot_id' : data[fields['id']], 'plant_barcode' : data[fields['plantbarcode']],
                     'camera_type' : camera_type, 'perspective' : perspective, 'rotation_angle' : rotation_angle,
                     'zoom' : zoom, 'imager_stage_vertical_position' : stage_position, 'camera_gain' : camera_gain,
                     'camera_exposure' : camera_exposure, 'image_id' : img_id, 'imagedate' : data[fields['timestamp']],
                     'species' : species, 'genotype' : genotype, 'treatment' : treatment,
                     'sample_id' : parsed_barcode['unique_id']}

    #metadata_json = json.dumps(file_metadata)
    #return metadata_json
    return file_metadata


if __name__ == '__main__':
    main()

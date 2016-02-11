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
    csvfile = open(args.dir + '/SnapshotInfo.csv', 'rU')

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
                # Build image file path
                image_path = posixpath.join(args.dir, 'snapshot' + data[colnames['id']], image_name)
                upload_file_to_clowder(sess, args.url, image_path, ds_id, args.dryrun)

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
        coll_r = session.post(url + "api/collections", headers={"Content-Type": "application/json"},
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
        ds_r = session.post(url + "api/datasets/createempty",  headers={"Content-Type": "application/json"},
                         data='{"name": "' + str(dataset) + '"}')

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
        coll_r = session.post(url + "api/collections/" + collection_id + "/datasets/" + ds_id)

        # Was the dataset added to the collection successfully?
        if coll_r.status_code != 200:
            raise StandardError("Adding dataset {0} to collection {1} failed: Return value = {2}".format(
                ds_id, collection_id, coll_r.status_code))

    # Add metadata to dataset
    if dryrun is False:
        meta_r = session.post(url + "api/datasets/" + ds_id + "/metadata", headers={"Content-Type": "application/json"},
                              data=json.dumps(metadata))

        # Was the metadata added to the dataset successfully?
        if meta_r.status_code != 200:
            raise StandardError("Adding metadata to dataset {0} failed: Return value = {1}".format(ds_id,
                                                                                                   meta_r.status_code))

    return ds_id
###########################################


# Upload file to a Clowder dataset
###########################################
def upload_file_to_clowder(session, url, file, dataset_id, dryrun=False):
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
            up_r = session.post(url + "api/uploadToDataset/" + dataset_id, files={"File" : f})

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
    parsed_barcode['genotype'] = barcode[2:5]
    parsed_barcode['treatment'] = barcode[5:7]
    parsed_barcode['unique_id'] = barcode[7:]
    return parsed_barcode

###########################################

if __name__ == '__main__':
    main()

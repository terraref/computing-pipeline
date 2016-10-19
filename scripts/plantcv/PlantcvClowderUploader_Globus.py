#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import json
import argparse
import posixpath
import json
import requests

"""

    1 Run Docker container including API endpoint for submission & Globus transfer components

        docker run -p 5455:5455 \
            -v /Users/mburnette/globus/monitor:/home/danforth/data \
            -v /Users/mburnette/globus/sorghum_pilot_dataset:/home/danforth/snapshots \
            -v /var/log:/var/log \
            maxzilla2/terra-gantry-monitor

    2 Execute this script on desired data to send

        python PlantcvClowderUploader.py -d "/Users/mburnette/globus/sorghum_pilot_dataset" -m "/Users/mburnette/globus/sorghum_pilot_dataset/sorghum_pilot_ddpsc_metadata.json"

"""

# Parse command-line arguments
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
    parser.add_argument("-m", "--meta", help="Experiment metadata file in JSON format", required=True)
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")

    args = parser.parse_args()

    if not os.path.exists(args.dir):
        raise IOError("Directory does not exist: {0}".format(args.dir))
    if not os.path.exists(args.dir + '/SnapshotInfo.csv'):
        raise IOError("The snapshot metadata file SnapshotInfo.csv does not exist in {0}".format(args.dir))
    if not os.path.exists(args.meta):
        raise IOError("The metadata file {0} does not exist".format(args.meta))

    return args


def main():
    # Get options
    args = options()

    # Read experiment metadata from JSON
    exp_metadata = json.load(open(args.meta, 'rU'))

    # Open CSV & get header line & remove whitespace
    csvfile = open(posixpath.join(args.dir, 'SnapshotInfo.csv'), 'rU')
    header = csvfile.readline().rstrip('\n').replace(" ", "")

    # Table column order
    cols = header.split(',')
    colnames = {}
    for i, col in enumerate(cols):
        colnames[col] = i

    # Read through the CSV file to identify datasets/snapshots
    dataset_count = 0
    for row in csvfile:
        # Create a list of data columns by splitting on commas
        data = row.rstrip('\n').split(',')

        # The tiles column has the list of image names for each snapshot - remove last item b/c is trailing semicolon
        img_list = data[colnames['tiles']][:-1]

        # Does this snapshot contain images?
        if len(img_list) > 0:
            snapshot_id = 'snapshot%s' % data[colnames['id']]
            imgs = img_list.split(';')

            # Prepare our transfer object that will initiate Globus job
            globus_transfer_object = {
                "space_id": "571fbfefe4b032ce83d96006",
                "paths": [],
                "file_metadata": {}
            }

            # Send a metadata.json file for dataset metadata
            ds_md_file_path = os.path.join(args.dir, snapshot_id, "metadata.json")
            exp_metadata['experiment']['snapshot_id'] =  data[colnames['id']]
            with open(ds_md_file_path, 'w') as dataset_metadata_file:
                dataset_metadata_file.write(json.dumps(exp_metadata['experiment']))
            globus_transfer_object['paths'].append(ds_md_file_path)

            # Add each image file to the Clowder dataset
            dataset_name = None
            for img in imgs:
                image_name = img + '.png'
                img_metadata = metadata_to_json(img, exp_metadata, data, colnames)
                dataset_name = img_metadata['imagedate']
                image_path = posixpath.join(args.dir, snapshot_id, image_name)

                globus_transfer_object['paths'].append(image_path)
                globus_transfer_object['file_metadata'][image_name] = json.dumps(img_metadata)

            # Use YYYY-MM-DD in ds name if we found imagedate, otherwise use snapshot ID as before
            if not dataset_name:
                dataset_name = 'ddpscIndoorSuite - %s' % snapshot_id
            else:
                # e.g. ddpscIndoorSuite - 2014-06-05__15-47-05-342
                dataset_name = dataset_name.replace(' ', '__').replace(':', '-').replace('.','-')
                dataset_name = 'ddpscIndoorSuite - %s' % dataset_name

            globus_transfer_object['dataset_name'] = dataset_name

            if args.verbose:
                print("sending transfer to API: "+str(globus_transfer_object), file=sys.stderr)
                
            send_files_to_globus_api(json.dumps(globus_transfer_object))

            dataset_count += 1
            if args.verbose:
                if dataset_count % 100 == 0:
                    print('Datasets uploaded: ' + str(dataset_count), file=sys.stderr)

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

# Call local monitor API to queue files for Globus transfer
def send_files_to_globus_api(transferobject):
    """
        Example POST content:
            {
                "path": "file1.txt",
                "md": {...metadata object...},
                "dataset_name": "snapshot123456",
                "space_id": "571fbfefe4b032ce83d96006"
            }
        ...or...
            {
                "paths": ["file1.txt", "file2.jpg", "file3.jpg"...],
                "file_metadata": {
                    "file1.txt": {...metadata object...},
                    "file2.jpg": {...metadata object...}
                }
                "dataset_name": "snapshot123456",
                "space_id": "571fbfefe4b032ce83d96006"
            }
        ...or...
            {
                "paths": ["file1.txt", "file2.jpg", "file3.jpg"...],
                "sensor_name": "VIS",
                "timestamp": "2016-06-29__10-28-43-323",
                "space_id": "571fbfefe4b032ce83d96006"
            }

        In the second example, resulting dataset is called "VIS - 2016-06-29__10-28-43-323"

        To associate metadata with the given dataset, include a "metadata.json" file.
    """

    api_url = "http://192.168.99.100:5455/files"
    requests.post(api_url, data=transferobject)


if __name__ == '__main__':
    main()


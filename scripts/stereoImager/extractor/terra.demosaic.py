import os
import logging
import imp

from config import *
import pyclowder.extractors as extractors

def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints

    #set logging
    logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)

    #connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName, messageType=messageType, processFileFunction=process_dataset,
                                   checkMessageFunction=check_message, rabbitmqExchange=rabbitmqExchange, rabbitmqURL=rabbitmqURL)

def check_message(parameters):
    # TODO: re-enable once this is merged into Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/pull-requests/883/overview
    # fetch metadata from dataset to check if we should remove existing entry for this extractor first
    md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    for m in md:
        if 'agent' in m and 'name' in m['agent']:
            if m['agent']['name'].find(extractorName) > -1:
                print("skipping, already done")
                return False
                #extractors.remove_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)

    # Check for a left and right file before beginning processing
    found_left = False
    found_right = False
    for f in parameters['filelist']:
        if 'filename' in f and f['filename'].endswith('_left.bin'):
            found_left = True
        elif 'filename' in f and f['filename'].endswith('_right.bin'):
            found_right = True

    if found_left and found_right:
        return True
    else:
        return False

def process_dataset(parameters):
    metafile, img_left, img_right, metadata = None, None, None, None

    # Get left/right files and metadata
    for f in parameters['files']:
        # First check metadata attached to dataset in Clowder for item of interest
        if f.endswith('_dataset_metadata.json'):
            all_dsmd = bin2tiff.load_json(f)
            for curr_dsmd in all_dsmd:
                if 'content' in curr_dsmd and 'lemnatec_measurement_metadata' in curr_dsmd['content']:
                    metafile = f
                    metadata = curr_dsmd['content']
        # Otherwise, check if metadata was uploaded as a .json file
        elif f.endswith('_metadata.json') and f.find('/_metadata.json') == -1 and metafile is None:
            metafile = f
            metadata = bin2tiff.load_json(metafile)
        elif f.endswith('_left.bin'):
            img_left = f
        elif f.endswith('_right.bin'):
            img_right = f
    if None in [metafile, img_left, img_right, metadata]:
        bin2tiff.fail('Could not find all of left/right/metadata.')
        return

    print("img_left: %s" % img_left)
    print("img_right: %s" % img_right)
    print("metafile: %s" % metafile)
    temp_out_dir = metafile.replace(os.path.basename(metafile), "")

    print("Determining image shapes")
    left_shape = bin2tiff.get_image_shape(metadata, 'left')
    right_shape = bin2tiff.get_image_shape(metadata, 'right')

    center_position = bin2tiff.get_position(metadata) # (x, y, z) in meters
    fov = bin2tiff.get_fov(metadata, center_position[2], left_shape) # (fov_x, fov_y) in meters; need to pass in the camera height to get correct fov
    left_position = [center_position[0]+bin2tiff.STEREO_OFFSET, center_position[1], center_position[2]]
    right_position = [center_position[0]-bin2tiff.STEREO_OFFSET, center_position[1], center_position[2]]
    left_gps_bounds = bin2tiff.get_bounding_box(left_position, fov) # (lat_max, lat_min, lng_max, lng_min) in decimal degrees
    right_gps_bounds = bin2tiff.get_bounding_box(right_position, fov)

    print("Creating demosaicked images")
    left_out = os.path.join(temp_out_dir, img_left[:-4] + '.jpg')
    left_image = bin2tiff.process_image(left_shape, img_left, left_out)
    right_out = os.path.join(temp_out_dir, img_right[:-4] + '.jpg')
    right_image = bin2tiff.process_image(right_shape, img_right, right_out)
    print("Uploading output JPGs to dataset")
    extractors.upload_file_to_dataset(left_out, parameters)
    extractors.upload_file_to_dataset(right_out, parameters)

    print("Creating geoTIFF images")
    left_tiff_out = os.path.join(temp_out_dir, img_left[:-4] + '.tif')
    bin2tiff.create_geotiff('left', left_image, left_gps_bounds, left_tiff_out)
    right_tiff_out = os.path.join(temp_out_dir, img_right[:-4] + '.tif')
    bin2tiff.create_geotiff('right', right_image, right_gps_bounds, right_tiff_out)
    print("Uploading output geoTIFFs to dataset")
    extractors.upload_file_to_dataset(left_tiff_out, parameters)
    extractors.upload_file_to_dataset(right_tiff_out, parameters)

    # Tell Clowder this is completed so subsequent file updates don't daisy-chain
    metadata = {
        "@context": {
            "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
        },
        "dataset_id": parameters["datasetId"],
        "content": {"status": "COMPLETED"},
        "agent": {
            "@type": "cat:extractor",
            "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
        }
    }
    extractors.upload_dataset_metadata_jsonld(mdata=metadata, parameters=parameters)

if __name__ == "__main__":
    global bin2tiffScript

    # Import demosaic script from configured location
    bin2tiff = imp.load_source('bin_to_geotiff', bin2tiffScript)

    main()
import os
import logging

from config import *
import pyclowder.extractors as extractors
import bin_to_geotiff

def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints

    #set logging
    logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)

    #connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName, messageType=messageType, processFileFunction=process_dataset,
                                   checkMessageFunction=check_message, rabbitmqExchange=rabbitmqExchange, rabbitmqURL=rabbitmqURL)

def check_message(parameters):
    # Check for a left and right file before beginning processing
    if len(parameters['filelist']) >= 2:
        return True
    else:
        return False

def process_dataset(parameters):
    # TODO: re-enable once this is merged into Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/pull-requests/883/overview
    # fetch metadata from dataset to check if we should remove existing entry for this extractor first
    md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    if len(md) > 0:
        for m in md:
            if 'agent' in m and 'name' in m['agent']:
                if m['agent']['name'].find(extractorName) > -1:
                    print("skipping, already done")
                    return
                    #extractors.remove_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    #    pass


    metafile, img_left, img_right = None, None, None

    # Get left/right files and metadata
    for f in parameters['files']:
        # First check metadata attached to dataset in Clowder
        if f.endswith('_dataset_metadata.json'):
            metafile = f
        # Otherwise, check if metadata was uploaded as a .json file
        elif f.endswith('_metadata.json') and f.find('/_metadata.json')==-1:
            metafile = f
        elif f.endswith('_left.bin'):
            img_left = f
        elif f.endswith('_right.bin'):
            img_right = f
    if None in [metafile, img_left, img_right]:
        demosaic.fail('Could not find all 3 required files.')

    print("img_left: %s" % img_left)
    print("img_right: %s" % img_right)
    print("metadata: %s" % metafile)

    print("Determining image shapes")
    metadata = bin_to_geotiff.load_json(metafile)
    left_shape = bin_to_geotiff.get_image_shape(metadata, 'left')
    right_shape = bin_to_geotiff.get_image_shape(metadata, 'right')
    print left_shape
    # We can leave in_dir blank because img_left/img_right contain full temp path now
    temp_out_dir = metafile.replace(os.path.basename(metafile), "")
    
    center_position = bin_to_geotiff.get_position(metadata) # (x, y, z) in meters
    fov = bin_to_geotiff.get_fov(metadata, center_position[2], left_shape) # (fov_x, fov_y) in meters; need to pass in the camera height to get correct fov

    left_position = [center_position[0]+bin_to_geotiff.STEREO_OFFSET, center_position[1], center_position[2]]
    right_position = [center_position[0]-bin_to_geotiff.STEREO_OFFSET, center_position[1], center_position[2]]

    left_gps_bounds = bin_to_geotiff.get_bounding_box(left_position, fov) # (lat_max, lat_min, lng_max, lng_min) in decimal degrees
    right_gps_bounds = bin_to_geotiff.get_bounding_box(right_position, fov)

    print("Creating demosaicked images")
    left_out = os.path.join(temp_out_dir, img_left[:-4] + '.jpg')
    left_image = bin_to_geotiff.process_image(left_shape, img_left, left_out)
    right_out = os.path.join(temp_out_dir, img_right[:-4] + '.jpg')
    right_image = bin_to_geotiff.process_image(right_shape, img_right, right_out)
    print("Uploading output JPGs to dataset")
    extractors.upload_file_to_dataset(left_out, parameters)
    extractors.upload_file_to_dataset(right_out, parameters)
    
    print("Creating geoTIFF images")
    left_tiff_out = os.path.join(temp_out_dir, img_left[:-4] + '.tif')
    bin_to_geotiff.create_geotiff('left', left_image, left_gps_bounds, left_tiff_out)
    right_tiff_out = os.path.join(temp_out_dir, img_right[:-4] + '.tif')
    bin_to_geotiff.create_geotiff('right', right_image, right_gps_bounds, right_tiff_out)
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
    main()
    
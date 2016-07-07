import os
import logging

from config import *
import pyclowder.extractors as extractors
import demosaic

def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints

    #set logging
    logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)

    #connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName, messageType=messageType, processFileFunction=process_dataset,
                                   checkMessageFunction=check_message, rabbitmqExchange=rabbitmqExchange, rabbitmqURL=rabbitmqURL)

def check_message(parameters):
    print("CM")
    print(parameters)
    # Check for a left and right file before beginning processing
    if len(parameters['filelist']) >= 2:
        return True
    else:
        return False

def process_dataset(parameters):
    print("PD")
    print(parameters)

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
        if f.find('_dataset_metadata.json') > -1:
            metafile = f
        # Otherwise, check if metadata was uploaded as a .json file
        elif f.find('_metadata.json') > -1 and f.find('/_metadata.json') == -1:
            metafile = f
        elif f.find('_left.bin') > -1:
            img_left = f
        elif f.find('_right.bin') > -1:
            img_right = f
    if None in [metafile, img_left, img_right]:
        demosaic.fail('Could not find all 3 required files.')

    print("img_left: %s" % img_left)
    print("img_right: %s" % img_right)
    print("metadata: %s" % metafile)

    print("Determining image shapes")
    metadata = demosaic.load_json(metafile)
    left_shape = demosaic.get_image_shape(metadata, 'left')
    right_shape = demosaic.get_image_shape(metadata, 'right')
    # We can leave in_dir blank because img_left/img_right contain full temp path now
    temp_out_dir = metafile.replace(os.path.basename(metafile), "")

    print("Creating demosaicked images")
    demosaic.process_image(img_left, left_shape, "", temp_out_dir)
    left_out = os.path.join(temp_out_dir, img_left[:-4] + '.jpg')
    demosaic.process_image(img_right, right_shape, "", temp_out_dir)
    right_out = os.path.join(temp_out_dir, img_right[:-4] + '.jpg')

    print("Uploading output JPGs to dataset")
    extractors.upload_file_to_dataset(left_out, parameters)
    extractors.upload_file_to_dataset(right_out, parameters)

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
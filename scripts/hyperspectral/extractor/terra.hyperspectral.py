import os
import shutil
import logging
import subprocess

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
    # Check for a left and right file before beginning processing
    if len(parameters['filelist']) >= 2:
        return True
    else:
        return False

def process_dataset(parameters):
    print("PD")
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

    # Find _raw and _raw.hdr files in dataset
    rawfile = None
    hdrfile = None
    for f in parameters['files']:
        if f[-4:] == "_raw":
            rawfile = f
        elif f[-8:] == "_raw.hdr":
            hdrfile = f

    if rawfile and hdrfile:
        # Copy hdrfile to same tmp folder as rawfile so script doesn't break
        # rawpath = rawfile[:-len(os.path.basename(rawfile))]
        # hdrcopy = os.path.join(rawpath, os.path.basename(hdrfile))
        # shutil.copyfile(hdrfile, hdrcopy)
        # print("moved .hdr file: %s" % hdrcopy)

        # Invoke terraref.sh
        print("found raw file: %s" % os.path.basename(rawfile))
        print("found hdr file: %s" % os.path.basename(hdrfile))
        outfile = rawfile.replace("_raw", ".nc4")
        print("invoking terraref.sh for: %s" % os.path.basename(outfile))
        subprocess.call(["./terraref.sh", "-i", rawfile, "-o", outfile])

        # Verify outfile exists and upload to clowder
        extractors.upload_file_to_dataset(outfile, parameters)

        # Tell Clowder this is completed so subsequent file uploads don't daisy-chain
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
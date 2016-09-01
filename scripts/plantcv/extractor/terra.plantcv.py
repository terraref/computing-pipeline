#!/usr/bin/env python
import imp
import re
import os
import logging
import requests
import json

from config import *
import pyclowder.extractors as extractors
import cv2
import plantcv as pcv

def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints

    #set logging
    logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)

    #connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName, messageType=messageType, processFileFunction=process_dataset,
        checkMessageFunction=check_message, rabbitmqExchange=rabbitmqExchange, rabbitmqURL=rabbitmqURL)

# ----------------------------------------------------------------------
def check_message(parameters):
    # For now if the dataset already has metadata from this extractor, don't recreate
    md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    if len(md) > 0:
        for m in md:
            if 'agent' in m and 'name' in m['agent']:
                if m['agent']['name'].find(extractorName) > -1:
                    print("skipping dataset %s, already processed" % parameters['datasetId'])
                    return False

    # Expect at least 10 relevant files to execute this processing
    relevantFiles = 0
    for f in parameters['filelist']:
        raw_name = re.findall(r"(VIS|NIR|vis|nir)_(SV|TV|sv|tv)(_\d+)*" , f["filename"])
        if raw_name != []:
            relevantFiles += 1

    if relevantFiles >= 10:
        return True
    else:
        return False

def process_dataset(parameters):
    # TODO: re-enable once this is merged into Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/pull-requests/883/overview
    # fetch metadata from dataset to check if we should remove existing entry for this extractor first
    # md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    # if len(md) > 0:
        #extractors.remove_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)

    (fields, traits) = pcia.get_traits_table()

    # get imgs paths, filter out the json paths
    img_paths = []
    for p in parameters['files']:
        if p[-4:] == '.jpg' or p[-4:] == '.png':
            img_paths.append(p)

        # Get metadata for avg_traits from file metadata
        elif p.endswith("_metadata.json") and not p.endswith("_dataset_metadata.json"):
            with open(p) as ds_md:
                md_set = json.load(ds_md)
                for md in md_set:
                    if 'content' in md:
                        needed_fields = ['plant_barcode', 'genotype', 'treatment', 'imagedate']
                        for fld in needed_fields:
                            if traits[fld] == '' and fld in md['content']:
                                if fld == 'imagedate':
                                    traits[fld] = md['content'][fld].replace(" ", "T")
                                else:
                                    traits[fld] = md['content'][fld]
                                

    # build list of file descriptor dictionaries with sensor info
    file_objs = []
    for f in parameters['filelist']:
        found_info = False
        image_id = f['id']
        # Get from file metadata if possible
        file_md = extractors.download_file_metadata_jsonld(parameters['host'], parameters['secretKey'], f['id'])
        for md in file_md:
            if 'content' in md:
                mdc = md['content']
                if ('rotation_angle' in mdc) and ('perspective' in mdc) and ('camera_type' in mdc):
                    found_info = True
                    # perspective = 'side-view' / 'top-view'
                    perspective = mdc['perspective']
                    # angle = -1, 0, 90, 180, 270; set top-view angle to be -1 for later sorting
                    angle = mdc['rotation_angle'] if perspective != 'top-view' else -1
                    # camera_type = 'visible/RGB' / 'near-infrared'
                    camera_type = mdc['camera_type']

                    for pth in img_paths:
                        if re.findall(str(image_id), pth) != []:
                            file_objs.append({
                                'perspective': perspective,
                                'angle': angle,
                                'camera_type': camera_type,
                                'image_path': pth,
                                'image_id': image_id
                            })

        if not found_info:
            # Get from filename if no metadata is found
            raw_name = re.findall(r"(VIS|NIR|vis|nir)_(SV|TV|sv|tv)(_\d+)*" , f["filename"])
            if raw_name != []:
                    raw_int = re.findall('\d+', raw_name[0][2])
                    angle = -1 if raw_int == [] else int(raw_int[0]) # -1 for top-view, else angle
                    camera_type = raw_name[0][0]
                    perspective = raw_name[0][1].lower()

                    for pth in img_paths:
                        if re.findall(str(image_id), pth) != []:
                            file_objs.append({
                                'perspective': 'side-view' if perspective == 'tv' else 'top-view',
                                'angle': angle,
                                'camera_type': 'visible/RGB' if camera_type == 'vis' else 'near-infrared',
                                'image_path': pth,
                                'image_id': image_id
                            })

    # sort file objs by angle
    file_objs = sorted(file_objs, key=lambda k: k['angle'])
    
    # process images by matching angles with plantcv
    for i in [x for x in range(len(file_objs)) if x % 2 == 0]:
        if file_objs[i]['camera_type'] == 'visible/RGB':
            vis_src = file_objs[i]['image_path']
            nir_src = file_objs[i+1]['image_path']
            vis_id = file_objs[i]['image_id']
            nir_id = file_objs[i+1]['image_id']
        else:
            vis_src = file_objs[i+1]['image_path']
            nir_src = file_objs[i]['image_path']
            vis_id = file_objs[i+1]['image_id']
            nir_id = file_objs[i]['image_id']
        print '...processing: %s + %s' % (os.path.basename(vis_src), os.path.basename(nir_src))

        # Read VIS image
        img, path, filename = pcv.readimage(vis_src)
        brass_mask = cv2.imread('masks/mask_brass_tv_z1_L1.png')
        # Read NIR image
        nir, path1, filename1 = pcv.readimage(nir_src)
        nir2 = cv2.imread(nir_src, -1)

        if i == 0:
            vn_traits = pcia.process_tv_images_core(vis_id, img, nir_id, nir, nir2, brass_mask, traits)
        else:
            vn_traits = pcia.process_sv_images_core(vis_id, img, nir_id, nir, nir2, traits)

        print "...uploading resulting metadata"
        # upload the individual file metadata
        metadata = {
            "@context": {
                "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
            },
            "content": vn_traits[0],
            "agent": {
                "@type": "cat:extractor",
                "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
            }
        }
        parameters["fileid"] = vis_id
        extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)
        metadata = {
            "@context": {
                "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
            },
            "content": vn_traits[1],
            "agent": {
                "@type": "cat:extractor",
                "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
            }
        }
        parameters["fileid"] = nir_id
        extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)

    # compose the summary traits
    trait_list = pcia.generate_traits_list(traits)

    # generate output CSV & send to Clowder + BETY
    outfile = 'avg_traits.csv'
    pcia.generate_average_csv(outfile, fields, trait_list)
    extractors.upload_file_to_dataset(outfile, parameters)
    submitToBety(outfile)
    os.remove(outfile)

    # Flag dataset as processed by extractor
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

def submitToBety(csvfile):
    global betyAPI, betyKey

    if betyAPI != "":
        sess = requests.Session()
        print(csvfile)
        print("%s?key=%s" % (betyAPI, betyKey))
        r = sess.post("%s?key=%s" % (betyAPI, betyKey),
                  data=file(csvfile, 'rb').read(),
                  headers={'Content-type': 'text/csv'})

        if r.status_code == 200 or r.status_code == 201:
            print("CSV successfully uploaded to BETYdb.")
        else:
            print("Error uploading CSV to BETYdb %s" % r.status_code)
            print(r.text)



if __name__ == "__main__":
    global scriptPath

    # Import PlantcvClowderIndoorAnalysis script from configured location
    pcia = imp.load_source('PlantcvClowderIndoorAnalysis', scriptPath)

    main()

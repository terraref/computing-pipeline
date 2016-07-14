#!/usr/bin/env python
import sys
import re
import os
import logging

from config import *
import pyclowder.extractors as extractors
import PlantcvClowderIndoorAnalysis as pci
import numpy as np


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
    # Expect at least 10 files to execute this processing
    if len(parameters['filelist']) >= 10:
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


    # Compiled traits table
    fields = ('plant_barcode', 'genotype', 'treatment', 'imagedate', 'sv_area', 'tv_area', 'hull_area',
              'solidity', 'height', 'perimeter')
    traits = {'plant_barcode' : '',
              'genotype' : '',
              'treatment' : '',
              'imagedate' : '',
              'sv_area' : [],
              'tv_area' : '',
              'hull_area' : [],
              'solidity' : [],
              'height' : [],
              'perimeter' : []}
    nir_traits = {}
    vis_traits = {}

    # build img paths list
    img_paths = []
    # get imgs paths, filter out the json paths
    for p in parameters['files']:
        if p[-4:] == '.jpg' or p[-4:] == '.png':
            img_paths.append(p)
    print "printing img_paths..."
    print img_paths

    # build file objs - list of dicts
    file_objs = []
    for f in parameters['filelist']:
        fmd = (extractors.download_file_metadata_jsonld(parameters['host'], parameters['secretKey'], f['id'], extractorName))[0]
        #print "printing fmd..."
        #print fmd
        angle = fmd['content']['rotation_angle']        # -1, 0, 90, 180, 270
        perspective = fmd['content']['perspective']        # 'side-view' / 'top-view'
        if perspective == 'top-view': angle = -1        # set tv angle to be -1 for later sorting        
        camera_type = fmd['content']['camera_type']        # 'visible/RGB' / 'near-infrared'
        image_id = f['id']
        for p in img_paths:
            print "printing p.."
            print p 
            path =  (re.findall(str(image_id), p))
            print "printing path founded.."
            print path
            if path != []:
                file_objs.append({'perspective':perspective, 'angle':angle, 'camera_type':camera_type, 'image_path': p, 'image_id': image_id})
    
    print "printing file objs.."
    print file_objs
    # sort file objs by angle
    file_objs = sorted(file_objs, key=lambda k: k['angle']) 
    print "printing sorted.."
    print file_objs
    
    # process images by matching angles with plantcv
    for i in [0,2,4,6,8]:
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
        print 'vis src: ' + vis_src
        print 'nir src: ' + nir_src

        if i == 0:
            vn_traits = pci.process_tv_images(vis_src, nir_src, traits)
        else:
            vn_traits = pci.process_sv_images(vis_src, nir_src, traits)
        print "finished processing..."
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
    trait_list = [  traits['plant_barcode'], 
                    traits['genotype'], 
                    traits['treatment'], 
                    traits['imagedate'],
                    np.mean(traits['sv_area']), 
                    traits['tv_area'], 
                    np.mean(traits['hull_area']),
                    np.mean(traits['solidity']), 
                    np.mean(traits['height']),
                    np.mean(traits['perimeter'])]


    outfile = 'avg_traits.csv'
    with open(outfile, 'w') as csv:
        csv.write(','.join(map(str, fields)) + '\n')
        csv.write(','.join(map(str, trait_list)) + '\n')
        csv.flush()
        extractors.upload_file_to_dataset(outfile, parameters)
    os.remove(outfile)

    # debug
    csv_data = ','.join(map(str, fields)) + '\n' + ','.join(map(str, trait_list)) + '\n'
    print csv_data

    metadata = {
        "@context": {
            "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
        },
        "dataset_id": parameters["datasetId"],
        "content": {"status": "COMPLETED", "csv": csv_data},
        "agent": {
            "@type": "cat:extractor",
            "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
        }
    }
    extractors.upload_dataset_metadata_jsonld(mdata=metadata, parameters=parameters)





if __name__ == "__main__":
    main()

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


# traits dict to store avg traits 
# TODO: keys commented out are those which are going to be defined by pre-uploaded metadata attributes, not available so far
    traits = {# 'plant_barcode' : '',
              # 'genotype' : '',
              # 'treatment' : '',
              # 'imagedate' : '',
              'sv_area' : [],
              'tv_area' : '',
              'hull_area' : [],
              'solidity' : [],
              'height' : [],
              'perimeter' : []}



    #### get file_objs, each elem contains 4 attribs: (nir_vis, sv_tv, angle, path)
    img_paths = []
    # get imgs paths, filter out the json paths
    for p in parameters['filelist']:
        # TODO just go through files again
        fname = p["filename"]
        fid = p['id']
        if fname.find(".jpg") > -1 or fname.find(".png") > -1:
            img_paths.append({"name": fname, "id": fid})

    if len(img_paths) >= 10 :
        # construct file_objs based on img paths, it has 4 attribs: 
        # nir_vis : 'nir' if the img is nir, 'vis' if the img is vis
        # sv_tv : 'sv' if the img is sv, 'tv' if the img is tv
        # angle : img rotation angle
        # p : img path in local host
        file_objs = []
        for p in img_paths:
            raw_name = re.findall(r"(VIS|NIR|vis|nir)_(SV|TV|sv|tv)(_\d+)*" , p["name"])
            if raw_name != []:
                raw_int = re.findall('\d+', raw_name[0][2])
                angle = -1 if raw_int == [] else int(raw_int[0]) # -1=TV, else SV
                nir_vis = raw_name[0][0]
                sv_tv = raw_name[0][1]
                file_objs.append((nir_vis, sv_tv, angle, p["name"], p["id"]))

        if file_objs == []:
            return

        # sort the file_objs by angle
        print 'printing sorted file objs...'
        file_objs = sorted(file_objs, key=lambda f: f[2])
        print file_objs

        # call Noah's script with matching angles
        for i in [0,2,4,6,8]:
            if (file_objs[i][0] == 'vis' ) or (file_objs[i][0] == 'VIS'):
                vis_src = file_objs[i][3]
                nir_src = file_objs[i+1][3]
                vis_id = file_objs[i][4]
                nir_id = file_objs[i+1][4]
            else:
                vis_src = file_objs[i+1][3]
                nir_src = file_objs[i][3]
                vis_id = file_objs[i+1][4]
                nir_id = file_objs[i][4]
            print 'vis src: ' + vis_src
            print 'nir src: ' + nir_src

            vis_path = extractFilePath(parameters['files'], vis_src)
            nir_path = extractFilePath(parameters['files'], nir_src)

            csv = ''

            # call plantcv script
            try:
                if i == 0:
                    csv = pci.process_tv_images(vis_path, nir_path, traits, debug=None)
                else:
                    csv = pci.process_sv_images(vis_path, nir_path, traits, debug=None)
            except ValueError:
                print 'pair num: '
                print i
                print("Oops!  That was no valid number.  Try again...")
                pass


            tempfile = vis_src.replace(".png", ".tab").replace(".jpg", ".tab")
            with open(tempfile, 'w') as temp:
                temp.write(csv)
                temp.flush()
                extractors.upload_file_to_dataset(tempfile, parameters)
            os.remove(tempfile)


            mdcontent = {
                'view': file_objs[i][1],
                'tab': csv
            }
            if angle > -1: mdcontent['angle'] = file_objs[i][2]

            # send csv as metadata to the dataset
            metadata = {
                "@context": {
                    "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
                },
                "content": mdcontent,
                "agent": {
                    "@type": "cat:extractor",
                    "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
                }
            }
            parameters["fileid"] = nir_id
            extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)
            parameters["fileid"] = vis_id
            extractors.upload_file_metadata_jsonld(mdata=metadata, parameters=parameters)

    # upload avg traits csv file
        fields = (# 'plant_barcode', 'genotype', 'treatment', 'imagedate', 
                'sv_area', 'tv_area', 'hull_area', 'solidity', 'height', 'perimeter')

        trait_list = [# traits['plant_barcode'], traits['genotype'], traits['treatment'], traits['imagedate'],
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
            "content": {"status": "COMPLETED"},
            "agent": {
                "@type": "cat:extractor",
                "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
            }
        }
        extractors.upload_dataset_metadata_jsonld(mdata=metadata, parameters=parameters)

# Return full path of file, given filename and list of file paths
def extractFilePath(listOfPaths, filename):
    for p in listOfPaths:
        if p.endswith(filename):
            return p



if __name__ == "__main__":
    main()

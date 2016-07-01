#!/usr/bin/env python
import sys
sys.path.append('./computing-pipeline/scripts/plantcv') #the directory that contains Noah's sccript
import re
import PlantcvClowderIndoorAnalysis as pci
import subprocess
import logging
from config import *
import pyclowder.extractors as extractors

def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL    

    #set logging
    logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)

    #connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName, messageType=messageType, processFileFunction=process_dataset,
        checkMessageFunction=check_message, rabbitmqExchange=rabbitmqExchange, rabbitmqURL=rabbitmqURL)

# ----------------------------------------------------------------------
def check_message(parameters):
    # This function can be used to evaluate the dataset contents (filenames, lengths, etc) before downloading
    if len(parameters['filelist']) > 0:
        # This dataset should be processed
        return True
    else:
        # Skip processing this dataset
        return False

def process_dataset(parameters):
    print("PD")
    print(parameters)
    # params
    num_files = len(parameters['filelist'])
    filename = parameters['filelist'][num_files-1]['filename']
    file_objs = []
    
    #DEBUG
    #print 'printing filelist length... '
    #print num_files
    #print 'printing params... '
    #print parameters
    #print 'printing fname...'
    #print filename

    # check if the image is valid
    #if re.match(r"^(VIS|NIR)_(SV|TV)(_\d+)*_z\d+_\d+\.(?:jpg|png)$", filename) is None:
        #print "This file is not my business, skipping..."
        #return
    #else:
        #print 'This is a valid img... '

    # TODO: re-enable once this is merged into Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/pull-requests/883/overview
    # fetch metadata from dataset to check if we should remove existing entry for this extractor first
    # md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    # if len(md) > 0:
        #extractors.remove_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    #    pass

    # sends metadata when # of files is 10
    if num_files == 10 :
        #### get file_objs, each elem contains 4 attribs: (nir_vis, sv_tv, angle, path)
        img_paths = []
		# get imgs paths, filter out the json paths
        for p in parameters['files']:
            #print 'printing p[-4:]..'
            #print p[-4:]
            #print re.findall('.(?:jpg|png)', p)
            if (p[-4:] == '.jpg') or (p[-4:] == '.png'):  
                #print 'added'
                img_paths.append(p)

        #print 'printing imgs paths..'
        #print img_paths

		# construct file_objs based on img paths, it has 4 attribs: 
		# nir_vis : 'nir' if the img is nir, 'vis' if the img is vis
		# sv_tv : 'sv' if the img is sv, 'tv' if the img is tv
		# angle : img rotation angle
		# p : img path in local host
        for p in img_paths:
            raw_name = re.findall(r"(VIS|NIR|vis|nir)_(SV|TV|sv|tv)(_\d+)*" , p)
            #print 'printing raw names..'
            #print raw_name
            raw_int = re.findall('\d+', raw_name[0][2])
            #print 'printing raw_int...'
            #print raw_int
            if raw_int == []:   # tv
                angle = -1
            else:               # sv
                angle = int(raw_int[0])
            nir_vis = raw_name[0][0]
            sv_tv = raw_name[0][1]
            file_objs.append((nir_vis, sv_tv, angle, p))

        #print 'printing file objs...'
        #print file_objs

        # sort the file_objs by angle
        print 'printing sorted file objs...'
        file_objs = sorted(file_objs, key=lambda f: f[2])
        print file_objs

        # call Noah's script with matching angles
        for i in [0,2,4,6,8]:
            if (file_objs[i][0] == 'vis' ) or (file_objs[i][0] == 'VIS'):
                vis_src = file_objs[i][3]
                nir_src = file_objs[i+1][3]
            else:
                vis_src = file_objs[i+1][3]
                nir_src = file_objs[i][3]             
            print 'vis src: ' + vis_src
            print 'nir src: ' + nir_src

            csv = ''

            # call plantcv script
            try:
                if i == 0: 
                    csv = ''   #EXCEPTION HERE, TO BE FIXED, pci.process_tv_images(vis_src, nir_src, debug=None)    
                else:                                                                
                    csv = pci.process_sv_images(vis_src, nir_src, debug=None)        
            except ValueError:                                                       
                print 'pair num: '                                                   
                print i
                print("Oops!  That was no valid number.  Try again...")
                pass
            
            # send csv as metadata to the dataset
            metadata = {
                "@context": {
                    "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
 	    	  	},
                "dataset_id": parameters["datasetId"],
                "content": {
                    'angle': file_objs[i][2],
                    'view': file_objs[i][1],
                    'csv': csv
 	    	  	},
                "agent": {
                    "@type": "cat:extractor",
                    "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
 	    	  	}
 	    	}
            extractors.upload_dataset_metadata_jsonld(mdata=metadata, parameters=parameters)

if __name__ == "__main__":
    main()

'''
    # store results as metadata
    metadata = {
        "@context": {
            "@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
        },
        "dataset_id": parameters["datasetId"],
        "content": {
            'last_file_added_name': parameters['filename'],
            'last_file_added_id': parameters['fileid']
        },
        "agent": {
            "@type": "cat:extractor",
            "extractor_id": parameters['host'] + "/api/extractors/" + extractorName
        }
    }

    extractors.upload_dataset_metadata_jsonld(mdata=metadata, parameters=parameters)
'''

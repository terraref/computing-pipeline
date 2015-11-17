#!/usr/bin/env python
import os
import requests
import json

# Clowder path and credentials
clowder_url      = 'http://141.142.208.144/clowder/'
clowder_username = ''
clowder_password = ''

sess = requests.Session()
sess.auth = (clowder_username, clowder_password)

# Folder containing files, and filenames to upload
# All files will be uploaded to one Dataset!
directory_path   = r"C:\Users\mburnet2\Documents\TERRAref\sample-data\Lemnatec-Indoor\fahlgren_et_al_2015_bellwether_jpeg\images\snapshot42549"
files_to_load    = os.listdir(directory_path) #["VIS_SV_0_z3500_307595.jpg"]

# First, create a Dataset for files to live
ds_r = sess.post(clowder_url+"api/datasets/createempty", headers={"Content-Type": "application/json"},
                     data='{"name": "Test Dataset"}')

# Did the request return success?
if ds_r.status_code == 200:
    ds_id = ds_r.json()["id"]
    print("Dataset created. Adding files...")
    
    for curr_file in files_to_load:
        if curr_file.find(".jpg") > -1:
            f = open(directory_path+"\\"+curr_file, 'rb')
            r = sess.post(clowder_url+"api/uploadToDataset/"+ds_id, files={"File" : f})

            # Did the request return success?
            if r.status_code == 200:
                print("Successfully uploaded "+curr_file+".")
                # TODO - Add metadata derived from filename
                
            else:
                print("ERROR: "+str(r))
                print("Failed to upload "+curr_file+".")
        
else:
        print("ERROR: "+str(ds_r))
        print("Failed to create dataset.")

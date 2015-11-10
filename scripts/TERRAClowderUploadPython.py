#!/usr/bin/env python
import requests
import json

# Clowder path and credentials
clowder_url      = 'http://141.142.208.144/clowder/'
clowder_username = ''
clowder_password = ''

# Folder containing files, and filenames to upload
directory_path   = r"C:\Users\mburnet2\Documents\TERRAref\sample-data\Lemnatec-Indoor\fahlgren_et_al_2015_bellwether_jpeg\images\snapshot42549"
files_to_load    = ["VIS_SV_0_z3500_307595.jpg"]

sess = requests.Session()
sess.auth = (clowder_username, clowder_password)
for curr_file in files_to_load:
    f = open(directory_path+"\\"+curr_file, 'rb')
    r = sess.post(clowder_url+"api/files", files={"File" : f})

    # Did the request return success?
    if r.status_code == 200:
        print("Successfully uploaded "+curr_file+".")
        # TODO - Add metadata derived from filename
        
    else:
        print("ERROR: "+str(r))
        print("Failed to upload "+curr_file+".")

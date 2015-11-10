#!/bin/bash
USER=""
PASS=""
FILE="C:\Users\mburnet2\Documents\TERRAref\sample-data\Lemnatec-Indoor\fahlgren_et_al_2015_bellwether_jpeg\images\snapshot42549\VIS_SV_0_z3500_307595.jpg"

curl -X POST -u $USER:$PASS -F "File=@$FILE;type=image/jpg" http://141.142.208.144/clowder/api/files
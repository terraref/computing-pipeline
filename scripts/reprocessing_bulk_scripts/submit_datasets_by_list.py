import argparse
import logging
import requests

from pyclowder.connectors import Connector
from pyclowder.datasets import submit_extraction

"""
This script will consume a CSV generated by get_dataset_ids_SENSOR.js and submit them to a desired extractor.
The -d flag is commonly used when submitting GeoTIFFs to rulechecker that have previously been recorded in order
to trigger a fieldmosaic process.

Usage:
        python submit_datasets_by_list.py -k CLOWDERKEY -f list_SENSOR_YEAR.csv -e terra.stereo-rgb.bin2tif

Sensor -> extractor reference:
    stereoTop                   -> terra.stereo-rgb.bin2tif
    RGB GeoTIFFs                -> ncsa.rulechecker.terra
    flirIrCamera                ->	terra.multispectral.flir2tif
    Thermal IR GeoTIFFs         -> ncsa.rulechecker.terra
    scanner3DTop                -> terra.3dscanner.ply2las
    Laser Scanner 3D LAS        -> terra.plotclipper
    VNIR                        -> terra.hyperspectral
    EnvironmentLogger netCDFs   -> terra.environmental.envlog2netcdf
"""

parser = argparse.ArgumentParser()
parser.add_argument('-k', '--key', help="Clowder key", default="")
parser.add_argument('-f', '--input', help="input CSV file")
parser.add_argument('-e', '--extractor', help="extractor to use", default="")
parser.add_argument('-s', '--sites', help="where /sites is mounted", default="/home/clowder/sites")
parser.add_argument('-h', '--host', help="Clowder host URL", default="https://terraref.ncsa.illinois.edu/clowder/")
parser.add_argument('-d', '--daily', help="only submit one dataset per day", default=False, action='store_true')
parser.add_argument('-t', '--test', help="only submit one dataset then exit", default=False, action='store_true')
args = parser.parse_args()

logging.basicConfig(filename="submit_%s.log" % args.input, level=logging.DEBUG)

CONN = Connector(None, mounted_paths={"/home/clowder/sites":args.sites})

logging.info("attempting to parse %s" % args.input)
sess = requests.Session()

if args.daily:
    seen_days = []
with open(args.input, 'r') as csv:
    i = 0
    for line in csv:
        ds_id, ds_name = line.replace("\n", "").split(",")
        if len(ds_id) > 0:
            if args.daily:
                day = ds_name.split(" - ")[1].split("__")[0]
                if day in seen_days:
                    continue
                else:
                    seen_days.append(day)
            try:
                submit_extraction(CONN, args.host, args.key, ds_id, args.extractor)
            except Exception as e:
                logging.info("failed to submit %s [%s]" % (ds_id, e))
        i+=1
        if (i % 1000 == 0):
            logging.info("submitted %s files" % i)
        if args.test:
            logging.info("submitted %s" % ds_id)
            break
logging.info("processing completed")
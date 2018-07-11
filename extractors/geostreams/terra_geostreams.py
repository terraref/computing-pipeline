#!/usr/bin/env python

import json
import csv

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info
from pyclowder.files import download_metadata, upload_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy
from terrautils.betydb import add_arguments, get_sites, get_sites_by_latlon, submit_traits, \
    get_site_boundaries
from terrautils.geostreams import create_datapoint_with_dependencies
from terrautils.gdal import clip_raster, centroid_from_geojson
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata



class GeostreamsUploader(TerrarefExtractor):
    def __init__(self):
        super(GeostreamsUploader, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='stereoTop_canopyCover')

    def check_message(self, connector, host, secret_key, resource, parameters):
        self.start_check(resource)

        md = download_metadata(connector, host, secret_key, resource['id'])
        if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
            self.log_skip(resource,"metadata indicates it was already processed")
            return CheckMessage.ignore
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        successful_plots = 0
        with open(resource['local_paths'][0], 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                centroid_lonlat = [row['lon'], row['lat']]
                time_fmt = row['dp_time']
                timestamp = row['timestamp']
                dpmetadata = {
                    "source": row['source'],
                    "value": row['value']
                }
                trait = row['trait']

                create_datapoint_with_dependencies(connector, host, secret_key, trait,
                                                   (centroid_lonlat[1], centroid_lonlat[0]), time_fmt, time_fmt,
                                                   dpmetadata, timestamp)
                successful_plots += 1

        # Add metadata to original dataset indicating this was run
        self.log_info(resource, "updating file metadata (%s)" % resource['id'])
        ext_meta = build_metadata(host, self.extractor_info, resource['id'], {
            "plots_processed": successful_plots,
        }, 'file')
        upload_metadata(connector, host, secret_key, resource['id'], ext_meta)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = GeostreamsUploader()
    extractor.start()

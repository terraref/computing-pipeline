#!/usr/bin/env python

import json
import re
import os
from numpy import asarray, rollaxis

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, get_info, upload_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy
from terrautils.betydb import add_arguments, get_sites, get_sites_by_latlon, submit_traits, \
    get_site_boundaries
from terrautils.geostreams import create_datapoint_with_dependencies
from terrautils.gdal import clip_raster, centroid_from_geojson
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata

import terraref.stereo_rgb


class BetyDBUploader(TerrarefExtractor):
    def __init__(self):
        super(BetyDBUploader, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='stereoTop_canopyCover')

        # assign other argumentse
        self.bety_url = self.args.bety_url
        self.bety_key = self.args.bety_key

    def check_message(self, connector, host, secret_key, resource, parameters):
        self.start_check(resource)

        md = download_metadata(connector, host, secret_key, resource['parent']['id'])
        if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
            self.log_skip(resource,"metadata indicates it was already processed")
            return CheckMessage.ignore
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # submit CSV to BETY
        self.log_info(resource, "submitting CSV to bety")
        submit_traits(resource['local_paths'][0], betykey=self.bety_key)

        # Add metadata to original dataset indicating this was run
        self.log_info(resource, "updating dataset metadata (%s)" % resource['parent']['id'])
        ext_meta = build_metadata(host, self.extractor_info, resource['parent']['id'], {
            "betydb_link": "https://terraref.ncsa.illinois.edu/bety/api/beta/variables?name=canopy_cover"
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['parent']['id'], ext_meta)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = BetyDBUploader()
    extractor.start()

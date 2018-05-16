#!/usr/bin/env python

import json
import csv
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



class GeostreamsUploader(TerrarefExtractor):
    def __init__(self):
        super(GeostreamsUploader, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='stereoTop_canopyCover')

    def check_message(self, connector, host, secret_key, resource, parameters):
        self.start_check(resource)

        md = download_metadata(connector, host, secret_key, resource['parent']['id'])
        if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
            self.log_skip(resource,"metadata indicates it was already processed")
            return CheckMessage.ignore
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # Write the CSV to the same directory as the source file
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]

        # Read CSV contents into dict
        trait_data = {}
        with open(resource['local_paths'][0], 'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                trait_data[row['site']] = {}
                for field in row:
                    if field != 'site':
                        trait_data[row['site']][field] = row[field]

        # Get full list of experiment plots using date as filter
        all_plots = get_site_boundaries(timestamp, city='Maricopa')
        self.log_info(resource, "found %s plots on %s" % (len(all_plots), timestamp))
        successful_plots = 0
        for plotname in all_plots:
            if plotname in trait_data:
                bounds = all_plots[plotname]

                # Prepare and submit datapoint
                centroid_lonlat = json.loads(centroid_from_geojson(bounds))["coordinates"]
                time_fmt = timestamp+"T12:00:00-07:00"
                dpmetadata = {"source": host + ("" if host.endswith("/") else "/") + "files/" + resource['id']}

                ignored_fields = ['site', 'citation_author', 'citation_year', 'citation_title', 'access_level', 'method']
                for field in trait_data[plotname]:
                    if field not in ignored_fields:
                        dpmetadata[field] = trait_data[plotname][field]
                if 'method' in trait_data[plotname]:
                    method_name = trait_data[plotname]['method']
                else:
                    # Get from last part of filename (e.g. _canopycover.csv)
                    method_name = resource['local_paths'][0].replace(".csv", "").split("_")[-1]

                create_datapoint_with_dependencies(connector, host, secret_key, method_name,
                                                   (centroid_lonlat[1], centroid_lonlat[0]), time_fmt, time_fmt,
                                                   dpmetadata, timestamp)
                successful_plots += 1
            else:
                self.log_error(resource, '%s not found in CSV' % plotname)

        # Add metadata to original dataset indicating this was run
        self.log_info(resource, "updating dataset metadata (%s)" % resource['parent']['id'])
        ext_meta = build_metadata(host, self.extractor_info, resource['parent']['id'], {
            "plots_processed": successful_plots,
            "plots_skipped": len(all_plots)-successful_plots
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['parent']['id'], ext_meta)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = GeostreamsUploader()
    extractor.start()

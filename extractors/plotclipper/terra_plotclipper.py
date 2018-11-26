#!/usr/bin/env python

import os
import yaml

from pyclowder.utils import CheckMessage
from pyclowder.files import download_metadata, upload_metadata
from pyclowder.datasets import download_metadata as download_dataset_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy
from terrautils.betydb import add_arguments, get_sites, get_sites_by_latlon, submit_traits, \
    get_site_boundaries
from terrautils.gdal import clip_raster, centroid_from_geojson, find_plots_intersect_boundingbox
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata
from terrautils.spatial import geojson_to_tuples_betydb

class PlotClipper(TerrarefExtractor):
    def __init__(self):
        super(PlotClipper, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='plotclipper')

    def check_message(self, connector, host, secret_key, resource, parameters):
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        files_to_process = {}
        # TODO: Iterate through files and identify GeoTIFFs or LAS files to process
        # files_to_process[filename] = local_path
        local_paths = resource['local_paths']

        for each_path in local_paths:
            file_name = os.path.basename(each_path)
            if file_name.endswith('.tiff'):
                files_to_process[file_name] = each_path

        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        all_plots = get_site_boundaries(timestamp.split("__")[0], city='Maricopa')

        if resource['type'] == 'dataset':
            # download dataset metadata
            dataset_md = download_dataset_metadata(connector, host, secret_key, resource['id'])
            if 'spatial_metadata' in dataset_md:
                spatial_metadata = dataset_md['spatial_metadata']
                if 'bounding_box' in spatial_metadata:
                    intersecting_plots = find_plots_intersect_boundingbox(spatial_metadata['bounding_box'], all_plots)
                elif 'left' in spatial_metadata and 'right' in spatial_metadata:
                    # TODO how to handle left and right?
                    intersecting_left = find_plots_intersect_boundingbox(spatial_metadata['left']['bounding_box'], all_plots)
                    intersecting_right = find_plots_intersect_boundingbox(spatial_metadata['right']['bounding_box'], all_plots)
                    intersecting_plots = intersecting_left

        for filename in files_to_process:
            self.log_info(resource, "Attempting to clip into plot shards")

            for plotname in intersecting_plots:
                if plotname.find("KSU") > -1:
                    continue

                bounds = all_plots[plotname]
                tuples = geojson_to_tuples_betydb(yaml.safe_load(bounds))

                out_img = self.sensors.create_sensor_path(timestamp, plot=plotname, filename=filename)

                if not os.path.exists(os.path.dirname(out_img)):
                    os.makedirs(os.path.dirname(out_img))
                clip_raster(files_to_process[filename], tuples, out_path=out_img)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = PlotClipper()
    extractor.start()

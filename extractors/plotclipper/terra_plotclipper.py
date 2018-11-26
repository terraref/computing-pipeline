#!/usr/bin/env python

import os
import yaml

from pyclowder.utils import CheckMessage
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy
from terrautils.betydb import add_arguments, get_sites, get_sites_by_latlon, submit_traits, \
    get_site_boundaries
from terrautils.gdal import clip_raster, centroid_from_geojson
from terrautils.spatial import geojson_to_tuples_betydb
from terrautils.metadata import get_terraref_metadata
from terrautils.gdal import find_plots_intersect_boundingbox

class PlotClipper(TerrarefExtractor):
    def __init__(self):
        super(PlotClipper, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='plotclipper')

    def check_message(self, connector, host, secret_key, resource, parameters):
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # Load metadata from dataset
        for fname in resource['local_paths']:
            if fname.endswith('_dataset_metadata.json'):
                all_dsmd = load_json_file(fname)
                terra_md_full = get_terraref_metadata(all_dsmd)
                if 'spatial_metadata' in terra_md_full:
                    spatial_meta = terra_md_full['spatial_metadata']
                else:
                    spatial_meta = None
        if not spatial_meta:
            ValueError("No spatial metadata found.")

        # Determine which files in dataset need clipping
        files_to_process = {}
        for f in resource['local_paths']:
            if f.startswith("ir_geotiff") and f.endswith(".tif"):
                filename = os.path.basename(f)
                files_to_process[filename] = {
                    "path": f,
                    "bounds": spatial_meta['flirIrCamera']['bounding_box']
                }

            elif f.startswith("rgb_geotiff") and f.endswith(".tif"):
                filename = os.path.basename(f)
                if f.endswith("_left.tif"): side = "left"
                else:                       side = "right"
                files_to_process[filename] = {
                    "path": f,
                    "bounds": spatial_meta[side]['bounding_box']
                }

            # TODO: Add case for laser3d LAS file
            # TODO: Add case for laser3d heightmap

        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        all_plots = get_site_boundaries(timestamp.split("__")[0], city='Maricopa')

        for filename in files_to_process:
            self.log_info(resource, "Attempting to clip into plot shards")

            file_path = files_to_process[filename]["path"]
            file_bounds = files_to_process[filename]["bounds"]
            overlap_plots = find_plots_intersect_boundingbox(file_bounds, all_plots)

            for plotname in overlap_plots:
                if plotname.find("KSU") > -1:
                    continue

                bounds = overlap_plots[plotname]
                tuples = geojson_to_tuples_betydb(yaml.safe_load(bounds))

                out_img = self.sensors.create_sensor_path(timestamp, plot=plotname, filename=filename)
                if not os.path.exists(os.path.dirname(out_img)):
                    os.makedirs(os.path.dirname(out_img))

                clip_raster(file_path, tuples, out_path=out_img)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = PlotClipper()
    extractor.start()

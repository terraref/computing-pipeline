#!/usr/bin/env python

import os
import yaml
import osr
import json
from osgeo import gdal, ogr

from pyclowder.utils import CheckMessage
from pyclowder.files import upload_to_dataset
from pyclowder.datasets import upload_metadata, remove_metadata, submit_extraction
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy_crawl, file_exists, check_file_in_dataset
from terrautils.betydb import add_arguments, get_sites, get_sites_by_latlon, submit_traits, \
    get_site_boundaries
from terrautils.spatial import geojson_to_tuples_betydb, \
    get_las_extents, clip_raster, clip_las, centroid_from_geojson
from terrautils.metadata import get_terraref_metadata, get_season_and_experiment


# TODO: Update to use terrautils 1.5 and py3 and remove these
def convert_geometry(geometry, new_spatialreference):
    """Converts the geometry to the new spatial reference if possible

    geometry - The geometry to transform
    new_spatialreference - The spatial reference to change to

    Returns:
        The transformed geometry or the original geometry. If either the
        new Spatial Reference parameter is None, or the geometry doesn't
        have a spatial reference, then the original geometry is returned.
    """
    if not new_spatialreference or not geometry:
        return geometry

    return_geometry = geometry
    try:
        geom_sr = geometry.GetSpatialReference()
        if geom_sr and not new_spatialreference.IsSame(geom_sr):
            transform = osr.CreateCoordinateTransformation(geom_sr, new_spatialreference)
            new_geom = geometry.Clone()
            if new_geom:
                new_geom.Transform(transform)
                return_geometry = new_geom
    except Exception as ex:
        logging.warning("Exception caught while transforming geometries: " + str(ex))
        logging.warning("    Returning original geometry")

    return return_geometry

def find_plots_intersect_boundingbox(bounding_box, all_plots, fullmac=True):
    """Take a list of plots from BETY and return only those overlapping bounding box.

    fullmac -- only include full plots (omit KSU, omit E W partial plots)

    """
    bbox_poly = ogr.CreateGeometryFromJson(str(bounding_box).replace("u'", "'").replace("'", '"'))
    bb_sr = bbox_poly.GetSpatialReference()
    intersecting_plots = dict()

    for plotname in all_plots:
        if fullmac and (plotname.find("KSU") > -1 or plotname.endswith(" E") or plotname.endswith(" W")):
            continue

        bounds = all_plots[plotname]

        yaml_bounds = yaml.safe_load(bounds)
        current_poly = ogr.CreateGeometryFromJson(json.dumps(yaml_bounds))

        # Check for a need to convert coordinate systems
        check_poly = current_poly
        if bb_sr:
            poly_sr = current_poly.GetSpatialReference()
            if poly_sr and not bb_sr.IsSame(poly_sr):
                # We need to convert to the same coordinate system before an intersection
                check_poly = convert_geometry(current_poly, bb_sr)
                transform = osr.CreateCoordinateTransformation(poly_sr, bb_sr)
                new_poly = current_poly.Clone()
                if new_poly:
                    new_poly.Transform(transform)
                    check_poly = new_poly

        intersection_with_bounding_box = bbox_poly.Intersection(check_poly)

        if intersection_with_bounding_box is not None:
            intersection = json.loads(intersection_with_bounding_box.ExportToJson())
            if 'coordinates' in intersection and len(intersection['coordinates']) > 0:
                intersecting_plots[plotname] = bounds

    return intersecting_plots

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
            filename = os.path.basename(f)
            bounds = None
            if filename.startswith("ir_geotiff") and filename.endswith(".tif"):
                sensor_name = "ir_geotiff"
                path = f
                bounds = spatial_meta['flirIrCamera']['bounding_box']['coordinates']
                files_to_process[filename] = {
                    "path": f,
                    "bounds": {
                        "type": "Polygon",
                        "coordinates": []
                    }
                }

            elif filename.startswith("rgb_geotiff") and filename.endswith(".tif"):
                sensor_name = "rgb_geotiff"
                if f.endswith("_left.tif"): side = "left"
                else:                       side = "right"
                path = f
                bounds = spatial_meta[side]['bounding_box']['coordinates']
                files_to_process[filename] = {
                    "path": f,
                    "bounds": {
                        "type": "Polygon",
                        "coordinates": []
                    }
                }

            elif filename.endswith(".las"):
                sensor_name = "laser3d_las"
                path = f
                bounds = get_las_extents(f)['coordinates']

            # TODO: Add case for laser3d heightmap

            if bounds is not None:
                # fix ordering
                bounds_fmt = [[
                    [bounds[0][1], bounds[0][0]],
                    [bounds[1][1], bounds[1][0]],
                    [bounds[2][1], bounds[2][0]],
                    [bounds[3][1], bounds[3][0]],
                    [bounds[0][1], bounds[0][0]]
                ]]

                files_to_process[filename] = {
                    "path": path,
                    "bounds": {
                        "type": "Polygon",
                        "coordinates": bounds_fmt
                    }
                }

        # Fetch experiment name from terra metadata
        timestamp = resource['dataset_info']['name'].split(" - ")[1]
        season_name, experiment_name, updated_experiment = get_season_and_experiment(timestamp, 'plotclipper', terra_md_full)
        if None in [season_name, experiment_name]:
            raise ValueError("season and experiment could not be determined")

        # Determine script name
        target_scan = "unknown_scan"
        if 'gantry_variable_metadata' in terra_md_full:
            if 'script_name' in terra_md_full['gantry_variable_metadata']:
                target_scan = terra_md_full['gantry_variable_metadata']['script_name']
                if 'script_hash' in terra_md_full['gantry_variable_metadata']:
                    target_scan += ' '+terra_md_full['gantry_variable_metadata']['script_hash']

        all_plots = get_site_boundaries(timestamp.split("__")[0], city='Maricopa')
        uploaded_file_ids = []

        for filename in files_to_process:
            file_path = files_to_process[filename]["path"]
            file_bounds = files_to_process[filename]["bounds"]

            overlap_plots = find_plots_intersect_boundingbox(file_bounds, all_plots, fullmac=True)

            if len(overlap_plots) > 0:
                self.log_info(resource, "Attempting to clip %s into %s plot shards" % (filename, len(overlap_plots)))
                for plotname in overlap_plots:
                    plot_bounds = overlap_plots[plotname]
                    tuples = geojson_to_tuples_betydb(yaml.safe_load(plot_bounds))

                    plot_display_name = self.sensors.get_display_name(sensor=sensor_name) + " (By Plot)"
                    leaf_dataset = plot_display_name + ' - ' + plotname + " - " + timestamp.split("__")[0]
                    self.log_info(resource, "Hierarchy: %s / %s / %s / %s / %s / %s / %s" % (season_name, experiment_name, plot_display_name,
                                                                                             timestamp[:4], timestamp[5:7], timestamp[8:10], leaf_dataset))
                    target_dsid = build_dataset_hierarchy_crawl(host, secret_key, self.clowder_user, self.clowder_pass, self.clowderspace,
                                                                season_name, experiment_name, plot_display_name,
                                                                timestamp[:4], timestamp[5:7], timestamp[8:10], leaf_ds_name=leaf_dataset)

                    out_file = self.sensors.create_sensor_path(timestamp, plot=plotname, subsensor=sensor_name, filename=filename)
                    if not os.path.exists(os.path.dirname(out_file)):
                        os.makedirs(os.path.dirname(out_file))

                    if filename.endswith(".tif") and (not file_exists(out_file) or self.overwrite):
                        """If file is a geoTIFF, simply clip it and upload it to Clowder"""
                        clip_raster(file_path, tuples, out_path=out_file, compress=True)

                        found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, out_file, remove=self.overwrite)
                        if not found_in_dest or self.overwrite:
                            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, out_file)
                            uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)
                        self.created += 1
                        self.bytes += os.path.getsize(out_file)

                    elif filename.endswith(".las"):
                        """If file is LAS, we can merge with any existing scan+plot output safely"""
                        merged_out = os.path.join(os.path.dirname(out_file), target_scan+"_merged.las")
                        merged_txt = merged_out.replace(".las", "_contents.txt")

                        already_merged = False
                        if os.path.exists(merged_txt):
                            # Check if contents
                            with open(merged_txt, 'r') as contents:
                                for entry in contents.readlines():
                                    if entry.strip() == file_path:
                                        already_merged = True
                                        break
                        if not already_merged:
                            clip_las(file_path, tuples, out_path=out_file, merged_path=merged_out)
                            with open(merged_txt, 'a') as contents:
                                contents.write(file_path+"\n")

                        # Upload the individual plot shards for optimizing las2height later
                        found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, out_file, remove=self.overwrite)
                        if not found_in_dest or self.overwrite:
                            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, out_file)
                            uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)
                        self.created += 1
                        self.bytes += os.path.getsize(out_file)

                        # Upload the merged result if necessary
                        found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, merged_out, remove=self.overwrite)
                        if not found_in_dest or self.overwrite:
                            fileid = upload_to_dataset(connector, host, secret_key, target_dsid, merged_out)
                            uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)
                        self.created += 1
                        self.bytes += os.path.getsize(merged_out)

                        # Trigger las2height extractor
                        submit_extraction(connector, host, secret_key, target_dsid, "terra.3dscanner.las2height")


        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        extractor_md = build_metadata(host, self.extractor_info, resource['id'], {
            "files_created": uploaded_file_ids
        }, 'dataset')
        self.log_info(resource, "uploading extractor metadata to Level_1 dataset")
        remove_metadata(connector, host, secret_key, resource['id'], self.extractor_info['name'])
        upload_metadata(connector, host, secret_key, resource['id'], extractor_md)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = PlotClipper()
    extractor.start()

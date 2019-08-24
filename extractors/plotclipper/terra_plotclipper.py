#!/usr/bin/env python

"""Extractor for clipping images and other sensor files to plot polygons
"""

import os
import sys
import logging
import yaml
import osr

from osgeo import ogr
from numpy import nan

import pyclowder.files as clowder_file

from pyclowder.utils import CheckMessage
from pyclowder.datasets import upload_metadata, remove_metadata, submit_extraction
from terrautils.extractors import TerrarefExtractor, load_json_file, confirm_clowder_info, \
    build_metadata, build_dataset_hierarchy_crawl, file_exists, check_file_in_dataset, \
     timestamp_to_terraref
from terrautils.betydb import get_site_boundaries
from terrautils.spatial import geojson_to_tuples_betydb, find_plots_intersect_boundingbox, \
    get_las_extents, clip_raster, clip_las, convert_geometry
from terrautils.metadata import get_terraref_metadata, prepare_pipeline_metadata
from terrautils.imagefile import file_is_image_type, image_get_geobounds, get_epsg

# The name of the BETYdb URL environment variable
BETYDB_URL_ENV_NAME = 'BETYDB_URL'

def find_betydb_url(metadata):
    """Performs a shalow search for a BETYdb URL in the metadata and
       returns it
    Args:
        metadata(dict): the metadata to search
    Return:
        The found BETYdb URL or None
    """
    if 'betydb' in metadata:
        if 'url' in metadata['betydb']:
            return metadata['betydb']['url']
    return None

def setup_betydb_url(url):
    """Sets up the BETYdb URL environemt variable. Returns the old
       value if it has been set
    Args:
        url(string): the BETYdb URL to set
    Return:
        The current BETYdb URL environment value or None if the
        variable was not set. If the variable is currently not set
        an empty string is returned.
    Exceptions:
        ValueError is thrown if the URL is not a string
    """
    if not url:
        return None

    if not isinstance(url, str):
        if sys.version_info[0] < 3:
            if isinstance(url, unicode):
                url = url.encode('ascii', 'ignore')
            else:
                ValueError("BETYdb URL is not a string")
        else:
            ValueError("BETYdb URL is not a string")

    old_url = os.environ.get(BETYDB_URL_ENV_NAME, '')
    os.environ[BETYDB_URL_ENV_NAME] = url

    return old_url

def get_terraref_files(resource, spatial_meta):
    """Looks throug the list of files for ones to process
    Args:
        resource(dict): dictionary containing the resources associated with the request
        spatial_meta(dict): dictionary of relevent spatial metadata
    Return:
        Returns a dict with filenames as keys. Each key has a dict value containing the
        full path to the file (key 'path') and boundary (key 'bounds')
    """
    found_files = {}
    for onefile in resource['local_paths']:
        if onefile.startswith("ir_geotiff") and onefile.endswith(".tif"):
            sensor_name = "ir_geotiff"
            filename = os.path.basename(onefile)
            found_files[filename] = {
                "path": onefile,
                "bounds": spatial_meta['flirIrCamera']['bounding_box'],
                "sensor": sensor_name
            }

        elif onefile.startswith("rgb_geotiff") and onefile.endswith(".tif"):
            sensor_name = "rgb_geotiff"
            filename = os.path.basename(onefile)
            if onefile.endswith("_left.tif"):
                side = "left"
            else:
                side = "right"
            found_files[filename] = {
                "path": onefile,
                "bounds": spatial_meta[side]['bounding_box'],
                "sensor": sensor_name
            }

        elif onefile.endswith(".las"):
            sensor_name = "laser3d_las"
            filename = os.path.basename(onefile)
            found_files[filename] = {
                "path": onefile,
                "bounds": get_las_extents(onefile),
                "sensor": sensor_name
            }
        # TODO: Add case for laser3d heightmap

    return found_files

class PlotClipper(TerrarefExtractor):
    """Extractor class for clipping sensor files to plot polygons
    """
    def __init__(self):
        super(PlotClipper, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='plotclipper')

    # List of file extensions we will probably see that we don't need to check for being
    # an image type
    @property
    def known_non_image_ext(self):
        """Returns an array of file extensions that we will see that
           are definitely not an image type
        """
        return ["dbf", "json", "prj", "shp", "shx", "txt"]

    # Look through the file list to find the files we need
    def find_image_files(self, files):
        """Finds geo referenced image files

        Args:
            files(list): the list of file to look through and access

        Returns:
            Returns  dict of georeferenced image files (indexed by filename and containing an
            object with the calculated image bounds as an ogr polygon and a list of the
            bounds as a tuple)

            The bounds are assumed to be rectilinear with the upper-left corner directly
            pulled from the file and the lower-right corner calculated based upon the geometry
            information stored in the file.

            The polygon points start at the upper left corner and proceed clockwise around the
            boundary. The returned polygon is closed: the first and last point are the same.

            The bounds tuple contains the min and max Y point values, followed by the min and
            max X point values.
        """
        found_files = {}

        for onefile in files:
            ext = os.path.splitext(os.path.basename(onefile))[1].lstrip('.')
            if not ext in self.known_non_image_ext:
                if file_is_image_type(self.args.identify_binary, onefile,
                                      onefile + self.file_infodata_file_ending):
                    # If the file has a geo shape we store it for clipping
                    bounds = image_get_geobounds(onefile)
                    epsg = get_epsg(onefile)
                    if bounds[0] != nan:
                        ring = ogr.Geometry(ogr.wkbLinearRing)
                        ring.AddPoint(bounds[2], bounds[1])     # Upper left
                        ring.AddPoint(bounds[3], bounds[1])     # Upper right
                        ring.AddPoint(bounds[3], bounds[0])     # lower right
                        ring.AddPoint(bounds[2], bounds[0])     # lower left
                        ring.AddPoint(bounds[2], bounds[1])     # Closing the polygon

                        poly = ogr.Geometry(ogr.wkbPolygon)
                        poly.AddGeometry(ring)

                        ref_sys = osr.SpatialReference()
                        if ref_sys.ImportFromEPSG(int(epsg)) != ogr.OGRERR_NONE:
                            logging.error("Failed to import EPSG %s for image file %s",
                                          str(epsg), onefile)
                        else:
                            poly.AssignSpatialReference(ref_sys)

                        sensor_name = "rgb_geotiff"
                        filename = os.path.basename(onefile)
                        found_files[filename] = {'path': onefile,
                                                 'bounds': poly,
                                                 "sensor": sensor_name
                                                }

        # Return what we've found
        return found_files

    def load_all_plots(self, datestamp):
        """Loads all the plots as requested from the appropriate source
        Args:
            datestamp(str): The date to use in TERRA REF format
        Returns:
            A dict of plot names as keys with geometries as the values
        """
        # Handle TERRA REF first
        if self.terraref_metadata:
            return get_site_boundaries(datestamp, city='Maricopa')

        # Look for configured site information
        if self.experiment_metadata:
            opts = {}
            if 'extractors' in self.experiment_metadata:
                extractor_json = self.experiment_metadata['extractors']
                if self.sensor_name in extractor_json:
                    if 'betydb_opts' in extractor_json[self.sensor_name]:
                        opts_list = extractor_json[self.sensor_name]['betydb_opts'].split(',')
                        for one_opt in opts_list:
                            idx = one_opt.find('=')
                            if idx < 0:
                                opts[one_opt] = ''
                            elif idx > 0:
                                opts[one_opt[0:idx]] = one_opt[idx+1:]
                            # We ignore any options starting with '='
            return get_site_boundaries(datestamp, **opts)

        return {}

    def check_message(self, connector, host, secret_key, resource, parameters):
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)
        super(PlotClipper, self).process_message(connector, host, secret_key, resource, parameters)

        # Load metadata from dataset
        if self.terraref_metadata:
            if 'spatial_metadata' in self.terraref_metadata:
                spatial_meta = self.terraref_metadata['spatial_metadata']
            else:
                ValueError("No spatial metadata found.")

        # Get the best username, password, and space
        old_un, old_pw, old_space = (self.clowder_user, self.clowder_pass, self.clowderspace)
        self.clowder_user, self.clowder_pass, self.clowderspace = self.get_clowder_context(host, secret_key)

        # Ensure that the clowder information is valid
        if not confirm_clowder_info(host, secret_key, self.clowderspace, self.clowder_user,
                                    self.clowder_pass):
            self.log_error(resource, "Clowder configuration is invalid. Not processing request")
            self.clowder_user, self.clowder_pass, self.clowderspace = (old_un, old_pw, old_space)
            self.end_message(resource)
            return

        # Change the base path of files to include the user by tweaking the sensor's value
        sensor_old_base = None
        if self.get_terraref_metadata is None:
            _, new_base = self.get_username_with_base_path(host, secret_key, resource['id'],
                                                           self.sensors.base)
            sensor_old_base = self.sensors.base
            self.sensors.base = new_base

        # Check if a new BETYdb URL has been specified
        if self.experiment_metadata:
            old_betydb_url = setup_betydb_url(find_betydb_url(self.experiment_metadata))
        else:
            old_betydb_url = None

        # Determine which files in dataset need clipping
        if self.terraref_metadata:
            files_to_process = get_terraref_files(resource, spatial_meta)
        else:
            files_to_process = self.find_image_files(resource['local_paths'])

        try:
            # Build up a list of image IDs
            image_ids = {}
            if 'files' in resource:
                for one_image in files_to_process:
                    image_name = os.path.basename(one_image)
                    for res_file in resource['files']:
                        if ('filename' in res_file) and ('id' in res_file) and \
                                                            (image_name == res_file['filename']):
                            image_ids[image_name] = res_file['id']

            # Get the best timestamp
            timestamp = timestamp_to_terraref(self.find_timestamp(resource['dataset_info']['name']))
            datestamp = timestamp.split("__")[0]

            # Get season and experiment names
            season_name, experiment_name, _ = self.get_season_and_experiment(timestamp, self.sensor_name)
            if None in [season_name, experiment_name]:
                # The finally block below restores changes instance variables
                raise ValueError("season and experiment could not be determined")

            # Determine script name
            if self.terraref_metadata:
                target_scan = "unknown_scan"
                if 'gantry_variable_metadata' in self.terraref_metadata:
                    if 'script_name' in self.terraref_metadata['gantry_variable_metadata']:
                        target_scan = self.terraref_metadata['gantry_variable_metadata']['script_name']
                        if 'script_hash' in self.terraref_metadata['gantry_variable_metadata']:
                            target_scan += ' '+self.terraref_metadata['gantry_variable_metadata']['script_hash']
            else:
                target_scan = ""

            all_plots = self.load_all_plots(datestamp, resource)
            file_filters = self.get_filters()
            uploaded_file_ids = []

            for filename in files_to_process:

                # Check if we're filtering files
                if file_filters:
                    if not file_filtered_in(filename, file_filters):
                        continue

                file_path = files_to_process[filename]["path"]
                file_bounds = files_to_process[filename]["bounds"]
                sensor_name = files_to_process[filename]["sensor"]

                overlap_plots = find_plots_intersect_boundingbox(file_bounds, all_plots, fullmac=True)
                num_plots = len(overlap_plots)

                if num_plots > 0:
                    self.log_info(resource, "Attempting to clip %s into %s plot shards" % (filename, len(overlap_plots)))
                    for plotname in overlap_plots:
                        plot_bounds = convert_geometry(overlap_plots[plotname], file_bounds.GetSpatialReference())
                        tuples = geojson_to_tuples_betydb(yaml.safe_load(plot_bounds))

                        plot_display_name = self.sensors.get_display_name(sensor=sensor_name) + " (By Plot)"
                        leaf_dataset = plot_display_name + ' - ' + plotname + " - " + datestamp
                        self.log_info(resource, "Hierarchy: %s / %s / %s / %s / %s / %s / %s" % \
                                                            (season_name, experiment_name, plot_display_name,
                                                             timestamp[:4], timestamp[5:7], timestamp[8:10], leaf_dataset))
                        ds_exists = dsid_by_name(host, secret_key, leaf_dataset)
                        target_dsid = build_dataset_hierarchy_crawl(host, secret_key, self.clowder_user, self.clowder_pass,
                                                                    self.clowderspace, season_name, experiment_name,
                                                                    plot_display_name, timestamp[:4], timestamp[5:7],
                                                                    timestamp[8:10], leaf_ds_name=leaf_dataset)
                        if (self.overwrite_ok or not ds_exists) and self.experiment_metadata:
                            self.update_dataset_extractor_metadata(connector, host, secret_key,
                                                                   target_dsid,
                                                                   prepare_pipeline_metadata(self.experiment_metadata),
                                                                   self.extractor_info['name'])

                        out_file = self.sensors.create_sensor_path(timestamp, plot=plotname, subsensor=sensor_name,
                                                                   filename=filename)
                        if not os.path.exists(os.path.dirname(out_file)):
                            os.makedirs(os.path.dirname(out_file))

                        if filename.endswith(".tif") and (not file_exists(out_file) or self.overwrite_ok):
                            # If file is a geoTIFF, simply clip it and upload it to Clowder
                            clip_raster(file_path, tuples, out_path=out_file, compress=True)

                            found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, out_file,
                                                                  remove=self.overwrite_ok)
                            if not found_in_dest or self.overwrite_ok:
                                content = {
                                    "comment": "Clipped from image file '" + filename + "'"
                                }
                                if filename in image_ids:
                                    content['imageFileId'] = image_ids[filename]
                                fileid = clowder_file.upload_to_dataset(connector, host, secret_key, target_dsid, out_file)
                                meta = build_metadata(host, self.extractor_info, fileid, content, 'file')
                                clowder_file.upload_metadata(connector, host, secret_key, fileid, meta)
                                uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)
                            self.created += 1
                            self.bytes += os.path.getsize(out_file)

                        elif filename.endswith(".las"):
                            # If file is LAS, we can merge with any existing scan+plot output safely
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
                            found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, out_file,
                                                                  remove=self.overwrite_ok)
                            if not found_in_dest or self.overwrite_ok:
                                fileid = clowder_file.upload_to_dataset(connector, host, secret_key, target_dsid, out_file)
                                uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)
                            self.created += 1
                            self.bytes += os.path.getsize(out_file)

                            # Upload the merged result if necessary
                            found_in_dest = check_file_in_dataset(connector, host, secret_key, target_dsid, merged_out,
                                                                  remove=self.overwrite_ok)
                            if not found_in_dest or self.overwrite_ok:
                                fileid = clowder_file.upload_to_dataset(connector, host, secret_key, target_dsid, merged_out)
                                uploaded_file_ids.append(host + ("" if host.endswith("/") else "/") + "files/" + fileid)
                            self.created += 1
                            self.bytes += os.path.getsize(merged_out)

                            # Trigger las2height extractor
                            submit_extraction(connector, host, secret_key, target_dsid, "terra.3dscanner.las2height")

            # Tell Clowder this is completed so subsequent file updates don't daisy-chain
            try:
                content = {
                    "files_created": uploaded_file_ids
                }
                if self.experiment_metadata:
                    content.update(prepare_pipeline_metadata(self.experiment_metadata))
                extractor_md = build_metadata(host, self.extractor_info, resource['id'], content, 'dataset')
                self.log_info(resource, "uploading extractor metadata to Level_1 dataset")
                remove_metadata(connector, host, secret_key, resource['id'], self.extractor_info['name'])
                upload_metadata(connector, host, secret_key, resource['id'], extractor_md)
            except Exception as ex:     # pylint: disable=broad-except
                self.log_error(resource, "Exception updating dataset metadata: " + str(ex))
        finally:
            # Signal end of processing message and restore changed variables. Be sure to restore
            # changed variables above with early returns
            if not old_betydb_url is None:
                setup_betydb_url(old_betydb_url)

            if not sensor_old_base is None:
                self.sensors.base = sensor_old_base

            self.clowder_user, self.clowder_pass, self.clowderspace = (old_un, old_pw, old_space)
            self.end_message(resource)

if __name__ == "__main__":
    extractor = PlotClipper()   # pylint: disable=invalid-name
    extractor.start()

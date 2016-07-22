#!/usr/bin/env python

'''
Created on May 3, 2016
Author: Joshua Little, Zongyang Li
This script takes in a folder that contains the metadata associated with a particular
stereo pair (*_metadata.json) and the binary stereo images (*_left.bin and *_right.bin),
and outputs demosaiced .jpg files and .tif files.
----------------------------------------------------------------------------------------
Usage:
python input_folder output_folder
where
input_folder        is the folder containing the metadata and binary stereo image inputs
output_folder     is the folder where the output .jpg files and .tif files will be saved
'''

import sys, os.path, json
from glob import glob
from os.path import join
import numpy as np
from scipy.ndimage.filters import convolve
from PIL import Image
from math import cos, pi
from osgeo import gdal, osr

ZERO_ZERO = (33.0745,-111.97475) # (latitude, longitude) of SE corner (positions are + in NW direction); I think this is EPSG4326 (wgs84)
# NOTE: This STEREO_OFFSET is an experimentally determined value.
STEREO_OFFSET = .17 # distance from center_position to each of the stereo cameras (left = +, right = -)

## PARAMS FROM 5/8
HEIGHT_MAGIC_NUMBER = 1.8 # this is the value we have to add to our Z position to get the images in a column to line up.

# Test by Baker
FOV_MAGIC_NUMBER = 0.1552 
FOV_IN_2_METER_PARAM = 0.837 # since we don't have a true value of field of view in 2 meters, we use this parameter(meter) to estimate fov in Y-

# PARAMS FROM 5/25
#HEIGHT_MAGIC_NUMBER = 1.3 # this is the value we have to add to our Z position to get the images in a column to line up.

def main(in_dir, out_dir, tif_list_file, bounds):
    if not os.path.isdir(in_dir):
        fail('Could not find input directory: ' + in_dir)
    if not os.path.isdir(out_dir):
        fail('Could not find output directory: ' + out_dir)

    metas, ims_left, ims_right = find_input_files(in_dir)

    for meta, im_left, im_right in zip(metas, ims_left, ims_right):
        metadata = lower_keys(load_json(join(in_dir, meta))) # make all our keys lowercase since keys appear to change case (???)

        left_shape = get_image_shape(metadata, 'left')
        right_shape = get_image_shape(metadata, 'right')

        center_position = get_position(metadata) # (x, y, z) in meters
        fov = get_fov(metadata, center_position[2], left_shape) # (fov_x, fov_y) in meters; need to pass in the camera height to get correct fov

        left_position = [center_position[0]+STEREO_OFFSET, center_position[1], center_position[2]]
        right_position = [center_position[0]-STEREO_OFFSET, center_position[1], center_position[2]]

        left_gps_bounds = get_bounding_box(left_position, fov) # (lat_max, lat_min, lng_max, lng_min) in decimal degrees
        right_gps_bounds = get_bounding_box(right_position, fov)

        # check if this file is in the GPS bounds of interest
        if left_gps_bounds[1] > bounds[0] and left_gps_bounds[0] < bounds[2] and left_gps_bounds[3] > bounds[1] and left_gps_bounds[2] < bounds[3]:
            left_file_path = join(in_dir, im_left)
            left_out = join(out_dir, 'left.jpg', )
            left_image = process_image(left_shape, left_file_path, left_out)
            right_file_path = join(in_dir, im_right)
            right_out = join(out_dir, 'right.jpg', )
            right_image = process_image(right_shape, right_file_path, right_out)

            left_tiff_out = join(out_dir,'left.tif')
            create_geotiff('left', left_image, left_gps_bounds, left_tiff_out)
            right_tiff_out = join(out_dir,'right.tif')
            create_geotiff('right', right_image, right_gps_bounds, right_tiff_out)

def lower_keys(in_dict):
    if type(in_dict) is dict:
        out_dict = {}
        for key, item in in_dict.items():
            out_dict[key.lower()] = lower_keys(item)
        return out_dict
    elif type(in_dict) is list:
        return [lower_keys(obj) for obj in in_dict]
    else:
        return in_dict

def find_input_files(in_dir):
    metadata_suffix = '_metadata.json'
    metas = [os.path.basename(meta) for meta in glob(join(in_dir, '*' + metadata_suffix))]
    if len(metas) == 0:
        fail('No metadata file found in input directory.')

    guids = [meta[:-len(metadata_suffix)] for meta in metas]
    ims_left = [guid + '_left.bin' for guid in guids]
    ims_right = [guid + '_right.bin' for guid in guids]

    return metas, ims_left, ims_right

def load_json(meta_path):
    try:
        with open(meta_path, 'r') as fin:
            return json.load(fin)
    except Exception as ex:
        fail('Corrupt metadata file, ' + str(ex))

def get_image_shape(metadata, which):
    try:
        im_meta = metadata['lemnatec_measurement_metadata']['sensor_variable_metadata']
        fmt = im_meta['image format %s image' % which]
        if fmt != 'BayerGR8':
            fail('Unknown image format: ' + fmt)
        width = im_meta['width %s image [pixel]' % which]
        height = im_meta['height %s image [pixel]' % which]
    except KeyError as err:
        fail('Metadata file missing key: ' + err.args[0])

    try:
        width = int(width)
        height = int(height)
    except ValueError as err:
        fail('Corrupt image dimension, ' + err.args[0])
    return (width, height)

def get_position(metadata):
    try:
        gantry_meta = metadata['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
        gantry_x = gantry_meta["position x [m]"]
        gantry_y = gantry_meta["position y [m]"]
        gantry_z = gantry_meta["position z [m]"]

        cam_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
        cam_x = cam_meta["location in camera box x [m]"]
        cam_y = cam_meta["location in camera box y [m]"]
        if "location in camera box z [m]" in cam_meta: # this may not be in older data
            cam_z = cam_meta["location in camera box z [m]"]
        else:
            cam_z = 0
    except KeyError as err:
        fail('Metadata file missing key: ' + err.args[0])

    try:
        x = float(gantry_x) + float(cam_x)
        y = float(gantry_y) + float(cam_y)
        z = HEIGHT_MAGIC_NUMBER + float(gantry_z) + float(cam_z) # gantry rails are at 2m
    except ValueError as err:
        fail('Corrupt positions, ' + err.args[0])
    return (x, y, z)

def get_fov(metadata, camHeight, shape):
    try:
        cam_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
        print cam_meta
        fov = cam_meta["field of view at 2m in X- Y- direction [m]"]
        print fov
    except KeyError as err:
        fail('Metadata file missing key: ' + err.args[0])

    try:
        fov_list = fov.replace("[","").replace("]","").split()
        fov_x = float(fov_list[0])
        fov_y = float(fov_list[1])
        
        # test by Baker
        gantry_meta = metadata['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
        gantry_z = gantry_meta["position z [m]"]
        fov_offset = (float(gantry_z) - 2) * FOV_MAGIC_NUMBER
        fov_y = fov_y*(FOV_IN_2_METER_PARAM + fov_offset)
        fov_x = (fov_y)/shape[1]*shape[0]

        # given fov is at 2m, so need to convert for actual height
        #fov_x = (camHeight * (fov_x/2))/2
        #fov_y = (camHeight * (fov_y/2))/2
        

    except ValueError as err:
        fail('Corrupt FOV inputs, ' + err.args[0])
    return (fov_x, fov_y)

def get_bounding_box(center_position, fov):
    # NOTE: ZERO_ZERO is the southeast corner of the field. Position values increase to the northwest (so +y-position = +latitude, or more north and +x-position = -longitude, or more west)
    # We are also simplifying the conversion of meters to decimal degrees since we're not close to the poles and working with small distances.

    # NOTE: x --> latitude; y --> longitude
    try:
        r = 6378137 # earth's radius

        x_min = center_position[1] - fov[1]/2
        x_max = center_position[1] + fov[1]/2
        y_min = center_position[0] - fov[0]/2
        y_max = center_position[0] + fov[0]/2

        lat_min_offset = y_min/r* 180/pi
        lat_max_offset = y_max/r * 180/pi
        lng_min_offset = x_min/(r * cos(pi * ZERO_ZERO[0]/180)) * 180/pi
        lng_max_offset = x_max/(r * cos(pi * ZERO_ZERO[0]/180)) * 180/pi

        lat_min = ZERO_ZERO[0] - lat_min_offset
        lat_max = ZERO_ZERO[0] - lat_max_offset
        lng_min = ZERO_ZERO[1] - lng_min_offset
        lng_max = ZERO_ZERO[1] - lng_max_offset
    except Exception as ex:
        fail('Failed to get GPS bounds from center + FOV: ' + str(ex))
    return (lat_max, lat_min, lng_max, lng_min)

def process_image(shape, in_file, out_file):
    try:
        im = np.fromfile(in_file, dtype='uint8').reshape(shape[::-1])
        im_color = demosaic(im)
        im_color = np.flipud(np.rot90(im_color))
        Image.fromarray(im_color).save(out_file)
        return im_color
    except Exception as ex:
        fail('Error processing image "%s": %s' % (str(ex)))

def demosaic(im):
    # Assuming GBRG ordering.
    B = np.zeros_like(im)
    R = np.zeros_like(im)
    G = np.zeros_like(im)
    R[0::2, 1::2] = im[0::2, 1::2]
    B[1::2, 0::2] = im[1::2, 0::2]
    G[0::2, 0::2] = im[0::2, 0::2]
    G[1::2, 1::2] = im[1::2, 1::2]

    fG = np.asarray(
            [[0, 1, 0],
             [1, 4, 1],
             [0, 1, 0]]) / 4.0
    fRB = np.asarray(
            [[1, 2, 1],
             [2, 4, 2],
             [1, 2, 1]]) / 4.0

    im_color = np.zeros(im.shape+(3,), dtype='uint8') #RGB
    im_color[:, :, 0] = convolve(R, fRB)
    im_color[:, :, 1] = convolve(G, fG)
    im_color[:, :, 2] = convolve(B, fRB)
    return im_color

def create_geotiff(which_im, np_arr, gps_bounds, out_file_path):
    try:
        nrows,ncols,nz = np.shape(np_arr)
        # gps_bounds: (lat_min, lat_max, lng_min, lng_max)
        xres = (gps_bounds[3] - gps_bounds[2])/float(ncols)
        yres = (gps_bounds[1] - gps_bounds[0])/float(nrows)
        geotransform = (gps_bounds[2],xres,0,gps_bounds[1],0,-yres) #(top left x, w-e pixel resolution, rotation (0 if North is up), top left y, rotation (0 if North is up), n-s pixel resolution)

        output_path = out_file_path

        output_raster = gdal.GetDriverByName('GTiff').Create(output_path, ncols, nrows, nz, gdal.GDT_Byte)

        output_raster.SetGeoTransform(geotransform) # specify coordinates
        srs = osr.SpatialReference() # establish coordinate encoding
        srs.ImportFromEPSG(4326) # specifically, google mercator
        output_raster.SetProjection( srs.ExportToWkt() ) # export coordinate system to file

        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        output_raster.GetRasterBand(1).WriteArray(np_arr[:,:,0].astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(1).FlushCache()
        output_raster.GetRasterBand(1).SetNoDataValue(-99)

        output_raster.GetRasterBand(2).WriteArray(np_arr[:,:,1].astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(2).FlushCache()
        output_raster.GetRasterBand(2).SetNoDataValue(-99)

        output_raster.GetRasterBand(3).WriteArray(np_arr[:,:,2].astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(3).FlushCache()
        output_raster.GetRasterBand(3).SetNoDataValue(-99)

        # once we've saved the image, make sure to append this path to our list of TIFs
        #f = open(tif_list_file,'a+')
        #f.write(output_path + '\n')
    except Exception as ex:
        fail('Error creating GeoTIFF: ' + str(ex))

def fail(reason):
    print >> sys.stderr, reason


if __name__ == '__main__':
    if len(sys.argv) != 4:
        fail('Usage: python %s <input_folder> <output_folder> <tif_list_file> <gps_bounds>' % sys.argv[0])
    retcode = main(*sys.argv[1:4])

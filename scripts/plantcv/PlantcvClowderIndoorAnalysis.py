#!/usr/bin/env python
from __future__ import print_function
import sys
import os
import argparse
import requests
import json
import posixpath
import cv2
import numpy as np
import plantcv as pcv


# Parse command-line arguments
###########################################
def options():
    """Parse command line options.

    Args:

    Returns:
        argparse object

    Raises:
        IOError: if image vis does not exist.
        IOError: if image nir does not exist.
    """

    parser = argparse.ArgumentParser(description="PlantCV Clowder image analysis script for the DDPSC indoor system.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    #parser.add_argument("-v", "--vis", help="Input VIS/RGB image.", required=True)
    #parser.add_argument("-n", "--nir", help="Input NIR image.", required=True)
    #parser.add_argument("-p", "--perspective", help="Camera perspective (side-view, top-view)", required=True)
    parser.add_argument("-d", "--dataset", help="Clowder Dataset key.", required=True)
    parser.add_argument("-u", "--url", help="Clowder URL.", required=True)
    parser.add_argument("-U", "--username", help="Clowder username.", required=True)
    parser.add_argument("-p", "--password", help="Clowder password.", required=True)

    args = parser.parse_args()

    # if not os.path.exists(args.vis):
    #     raise IOError("File does not exist: {0}".format(args.vis))
    # if not os.path.exists(args.nir):
    #     raise IOError("File does not exist: {0}".format(args.nir))

    return args


###########################################

# Main
###########################################
def main():
    """Main program.
    """

    # Get options
    args = options()

    # Create new session
    sess = requests.Session()
    sess.auth = (args.username, args.password)

    # Get list of files in dataset
    filelist = clowder_dataset_filelist(sess, args.url, args.dataset)

    # Build metadata set
    metadata = get_metadata(sess, args.url, filelist)

    (fields, traits) = get_traits_table()

    # Process images with PlantCV
    for perspective in metadata['visible/RGB'].keys():
        for rotation_angle in metadata['visible/RGB'][perspective].keys():
            # VIS/RGB image Clowder ID
            vis_id = metadata['visible/RGB'][perspective][rotation_angle]['img_id']
            # Matching NIR image Clowder ID
            nir_id = ''
            # Are there NIR images
            if 'near-infrared' in metadata:
                # Is there an NIR image with a matching camera perspective
                if perspective in metadata['near-infrared']:
                    # Is there an NIR image with a matching rotation angle
                    if rotation_angle in metadata['near-infrared'][perspective]:
                        nir_id = metadata['near-infrared'][perspective][rotation_angle]['img_id']
            if len(nir_id) == 0:
                # If no NIR image ID was found, raise an error
                raise StandardError("No NIR image found matching VIS image {0}".format(vis_id))

            # Add metadata to traits table
            traits['plant_barcode'] = metadata['visible/RGB'][perspective][rotation_angle]['content']['plant_barcode']
            traits['genotype'] = metadata['visible/RGB'][perspective][rotation_angle]['content']['genotype']
            traits['treatment'] = metadata['visible/RGB'][perspective][rotation_angle]['content']['treatment']
            # imagedate must be in format YYYY-MM-DDTHH:MM:SS.sss e.g. "2014-06-23T16:55:57.625"
            imgdate = metadata['visible/RGB'][perspective][rotation_angle]['content']['imagedate']
            if imgdate.find(" ") > -1: imgdate = imgdate.replace(" ", "T")
            traits['imagedate'] = imgdate

            if perspective == 'side-view':
                process_sv_images(sess, args.url, vis_id, nir_id, traits)
            elif perspective == 'top-view':
                process_tv_images(sess, args.url, vis_id, nir_id, traits)

    # Save traits table
    trait_list = pcv.generate_traits_list(traits)
    outfile = '%s.csv' % args.dataset
    generate_average_csv(outfile, fields, trait_list)
    upload_file_to_clowder(sess, args.url, outfile, args.dataset, {outfile : json.dumps({'type' : 'CSV traits table'})})
    os.remove(outfile)

# Utility functions for modularity between command line and extractors
###########################################
def get_traits_table():
    # Compiled traits table
    fields = ('entity', 'cultivar', 'treatment', 'local_datetime', 'sv_area', 'tv_area', 'hull_area',
              'solidity', 'height', 'perimeter', 'access_level', 'species', 'site')
    traits = {'plant_barcode' : '',
              'genotype' : '',
              'treatment' : '',
              'imagedate' : '',
              'sv_area' : [],
              'tv_area' : '',
              'hull_area' : [],
              'solidity' : [],
              'height' : [],
              'perimeter' : [],
              'access_level': '2',
              'species': 'Sorghum bicolor',
              'site': 'Danforth Plant Science Center Bellweather Phenotyping Facility'}

    return (fields, traits)

def generate_traits_list(traits):
    # compose the summary traits
    trait_list = [  traits['plant_barcode'],
                    traits['genotype'],
                    traits['treatment'],
                    traits['imagedate'],
                    average_trait(traits['sv_area']),
                    traits['tv_area'],
                    average_trait(traits['hull_area']),
                    average_trait(traits['solidity']),
                    average_trait(traits['height']),
                    average_trait(traits['perimeter']),
                    traits['access_level'],
                    traits['species'],
                    traits['site']
                ]

    return trait_list

def generate_average_csv(fname, fields, trait_list):
    """ Generate CSV called fname with fields and trait_list """
    csv = open(fname, 'w')
    csv.write(','.join(map(str, fields)) + '\n')
    csv.write(','.join(map(str, trait_list)) + '\n')
    csv.close()

    return fname

# Get list of files for a Clowder dataset
###########################################
def clowder_dataset_filelist(session, url, dataset):
    """Return a list of files for a Clowder Dataset.

    Inputs:
    session  = requests session object
    url      = Clowder URL
    dataset  = Clowder dataset key

    Returns:
    ret      = requests return object

    :param session: requests session object
    :param url: str
    :param dataset: str
    :return: requests return object
    """
    try:
        ret = session.get(posixpath.join(url, "api/datasets", dataset, "listFiles"))
    except session.exceptions.RequestException as e:
        print(e)
        sys.exit(1)

    return ret


# Get Clowder file metadata
###########################################
def clowder_file_metadata(session, url, fileid):
    """Get metadata for a file in Clowder.

    Inputs:
    session  = requests session object
    url      = Clowder URL
    fileid  = Clowder file key

    Returns:
    ret      = requests return object

    :param session: requests session object
    :param url: str
    :param fileid: str
    :return: requests return object
    """
    try:
        ret = session.get(posixpath.join(url, "api/files", fileid, "metadata.jsonld"))
    except session.exceptions.RequestException as e:
        print(e)
        sys.exit(1)

    return ret


# Build a metadata set for a dataset
###########################################
def get_metadata(session, url, filelist):
    """Build a metadata set for a Clowder dataset.

    Inputs:
    session  = requests session object
    url      = Clowder URL
    filelist = Clowder API response object for the datasets listFiles method

    Returns:
    metadata = Metadata dictionary

    :param session: requests session object
    :param url: str
    :param filelist: requests return object
    :return: metadata: dictionary
    """
    metadata = {}
    # Loop over the Clowder dataset image ID list
    for clowder_img in filelist.json():
        # Get metadata for the image from Clowder
        response = clowder_file_metadata(session, url, clowder_img['id'])
        # Metadata from multiple extractors may be present
        for extractor in response.json():
            # Find the extractor called "deprecatedapi" which refers to the API used to upload metadata
            if "user_id" in extractor['agent']:
                # Save a few metadata elements for convenience
                camera_type = extractor['content']['camera_type']
                perspective = extractor['content']['perspective']
                rotation_angle = extractor['content']['rotation_angle']
                # Store the image ID for later use
                extractor['img_id'] = clowder_img['id']
                if camera_type not in metadata:
                    metadata[camera_type] = {}
                if perspective not in metadata[camera_type]:
                    metadata[camera_type][perspective] = {}
                metadata[camera_type][perspective][rotation_angle] = extractor

    return metadata


def serialize_color_data(list):
    newlist = [float(x) for x in list]

    return newlist

# Process side-view images
###########################################
def process_sv_images(session, url, vis_id, nir_id, traits, debug=None):
    """Process side-view images from Clowder.

    Inputs:
    session = requests session object
    url     = Clowder URL
    vis_id  = The Clowder ID of an RGB image
    nir_img = The Clowder ID of an NIR grayscale image
    traits  = traits table (dictionary)
    debug   = None, print, or plot. Print = save to file, Plot = print to screen

    :param session: requests session object
    :param url: str
    :param vis_id: str
    :param nir_id: str
    :param traits: dict
    :param debug: str
    :return traits: dict
    """

    # Read VIS image from Clowder
    vis_r = session.get(posixpath.join(url, "api/files", vis_id), stream=True)
    img_array = np.asarray(bytearray(vis_r.content), dtype="uint8")
    img = cv2.imdecode(img_array, -1)

    # Read NIR image from Clowder
    nir_r = session.get(posixpath.join(url, "api/files", nir_id), stream=True)
    nir_array = np.asarray(bytearray(nir_r.content), dtype="uint8")
    nir = cv2.imdecode(nir_array, -1)
    nir_rgb = cv2.cvtColor(nir, cv2.COLOR_GRAY2BGR)

    [vis_traits, nir_traits] = process_sv_images_core(vis_id, img, nir_id, nir_rgb, nir, traits, debug)

    add_plantcv_metadata(session, url, vis_id, vis_traits)
    add_plantcv_metadata(session, url, nir_id, nir_traits)

    return traits

def process_sv_images_core(vis_id, vis_img, nir_id, nir_rgb, nir_cv2, traits, debug=None):
    # Pipeline step
    device = 0

    # Convert RGB to HSV and extract the Saturation channel
    device, s = pcv.rgb2gray_hsv(vis_img, 's', device, debug)

    # Threshold the Saturation image
    device, s_thresh = pcv.binary_threshold(s, 36, 255, 'light', device, debug)

    # Median Filter
    device, s_mblur = pcv.median_blur(s_thresh, 5, device, debug)
    device, s_cnt = pcv.median_blur(s_thresh, 5, device, debug)

    # Fill small objects
    # device, s_fill = pcv.fill(s_mblur, s_cnt, 0, device, args.debug)

    # Convert RGB to LAB and extract the Blue channel
    device, b = pcv.rgb2gray_lab(vis_img, 'b', device, debug)

    # Threshold the blue image
    device, b_thresh = pcv.binary_threshold(b, 137, 255, 'light', device, debug)
    device, b_cnt = pcv.binary_threshold(b, 137, 255, 'light', device, debug)

    # Fill small objects
    # device, b_fill = pcv.fill(b_thresh, b_cnt, 10, device, args.debug)

    # Join the thresholded saturation and blue-yellow images
    device, bs = pcv.logical_and(s_mblur, b_cnt, device, debug)

    # Apply Mask (for vis images, mask_color=white)
    device, masked = pcv.apply_mask(vis_img, bs, 'white', device, debug)

    # Convert RGB to LAB and extract the Green-Magenta and Blue-Yellow channels
    device, masked_a = pcv.rgb2gray_lab(masked, 'a', device, debug)
    device, masked_b = pcv.rgb2gray_lab(masked, 'b', device, debug)

    # Threshold the green-magenta and blue images
    device, maskeda_thresh = pcv.binary_threshold(masked_a, 127, 255, 'dark', device, debug)
    device, maskedb_thresh = pcv.binary_threshold(masked_b, 128, 255, 'light', device, debug)

    # Join the thresholded saturation and blue-yellow images (OR)
    device, ab = pcv.logical_or(maskeda_thresh, maskedb_thresh, device, debug)
    device, ab_cnt = pcv.logical_or(maskeda_thresh, maskedb_thresh, device, debug)

    # Fill small noise
    device, ab_fill1 = pcv.fill(ab, ab_cnt, 200, device, debug)

    # Dilate to join small objects with larger ones
    device, ab_cnt1 = pcv.dilate(ab_fill1, 3, 2, device, debug)
    device, ab_cnt2 = pcv.dilate(ab_fill1, 3, 2, device, debug)

    # Fill dilated image mask
    device, ab_cnt3 = pcv.fill(ab_cnt2, ab_cnt1, 150, device, debug)
    device, masked2 = pcv.apply_mask(masked, ab_cnt3, 'white', device, debug)

    # Convert RGB to LAB and extract the Green-Magenta and Blue-Yellow channels
    device, masked2_a = pcv.rgb2gray_lab(masked2, 'a', device, debug)
    device, masked2_b = pcv.rgb2gray_lab(masked2, 'b', device, debug)

    # Threshold the green-magenta and blue images
    device, masked2a_thresh = pcv.binary_threshold(masked2_a, 127, 255, 'dark', device, debug)
    device, masked2b_thresh = pcv.binary_threshold(masked2_b, 128, 255, 'light', device, debug)

    device, masked2a_thresh_blur = pcv.median_blur(masked2a_thresh, 5, device, debug)
    device, masked2b_thresh_blur = pcv.median_blur(masked2b_thresh, 13, device, debug)

    device, ab_fill = pcv.logical_or(masked2a_thresh_blur, masked2b_thresh_blur, device, debug)

    # Identify objects
    device, id_objects, obj_hierarchy = pcv.find_objects(masked2, ab_fill, device, debug)

    # Define ROI
    device, roi1, roi_hierarchy = pcv.define_roi(masked2, 'rectangle', device, None, 'default', debug, True, 700,
                                                 0, -600, -300)

    # Decide which objects to keep
    device, roi_objects, hierarchy3, kept_mask, obj_area = pcv.roi_objects(vis_img, 'partial', roi1, roi_hierarchy,
                                                                           id_objects, obj_hierarchy, device,
                                                                           debug)

    # Object combine kept objects
    device, obj, mask = pcv.object_composition(vis_img, roi_objects, hierarchy3, device, debug)

    ############## VIS Analysis ################
    # Find shape properties, output shape image (optional)
    device, shape_header, shape_data, shape_img = pcv.analyze_object(vis_img, vis_id, obj, mask, device, debug)

    # Shape properties relative to user boundary line (optional)
    device, boundary_header, boundary_data, boundary_img1 = pcv.analyze_bound(vis_img, vis_id, obj, mask, 384, device,
                                                                              debug)

    # Determine color properties: Histograms, Color Slices and
    # Pseudocolored Images, output color analyzed images (optional)
    device, color_header, color_data, color_img = pcv.analyze_color(vis_img, vis_id, mask, 256, device, debug,
                                                                    None, 'v', 'img', 300)

    # Output shape and color data
    vis_traits = {}
    for i in range(1, len(shape_header)):
        vis_traits[shape_header[i]] = shape_data[i]
    for i in range(1, len(boundary_header)):
        vis_traits[boundary_header[i]] = boundary_data[i]
    for i in range(2, len(color_header)):
        vis_traits[color_header[i]] = serialize_color_data(color_data[i])


    ############################# Use VIS image mask for NIR image#########################
    # Flip mask
    device, f_mask = pcv.flip(mask, "vertical", device, debug)

    # Reize mask
    device, nmask = pcv.resize(f_mask, 0.1154905775, 0.1154905775, device, debug)

    # position, and crop mask
    device, newmask = pcv.crop_position_mask(nir_rgb, nmask, device, 30, 4, "top", "right", debug)

    # Identify objects
    device, nir_objects, nir_hierarchy = pcv.find_objects(nir_rgb, newmask, device, debug)

    # Object combine kept objects
    device, nir_combined, nir_combinedmask = pcv.object_composition(nir_rgb, nir_objects, nir_hierarchy, device, debug)

    ####################################### Analysis #############################################
    device, nhist_header, nhist_data, nir_imgs = pcv.analyze_NIR_intensity(nir_cv2, nir_id, nir_combinedmask, 256,
                                                                           device, False, debug)
    device, nshape_header, nshape_data, nir_shape = pcv.analyze_object(nir_cv2, nir_id, nir_combined, nir_combinedmask,
                                                                       device, debug)

    nir_traits = {}
    for i in range(1, len(nshape_header)):
        nir_traits[nshape_header[i]] = nshape_data[i]
    for i in range(2, len(nhist_header)):
        nir_traits[nhist_header[i]] = serialize_color_data(nhist_data[i])

    # Add data to traits table
    traits['sv_area'].append(vis_traits['area'])
    traits['hull_area'].append(vis_traits['hull-area'])
    traits['solidity'].append(vis_traits['solidity'])
    traits['height'].append(vis_traits['height_above_bound'])
    traits['perimeter'].append(vis_traits['perimeter'])

    return [vis_traits, nir_traits]

# Process top-view images
###########################################
def process_tv_images(session, url, vis_id, nir_id, traits, debug=False):
    """Process top-view images.

    Inputs:
    session = requests session object
    url     = Clowder URL
    vis_id  = The Clowder ID of an RGB image
    nir_img = The Clowder ID of an NIR grayscale image
    traits  = traits table (dictionary)
    debug   = None, print, or plot. Print = save to file, Plot = print to screen.

    :param session: requests session object
    :param url: str
    :param vis_id: str
    :param nir_id: str
    :param traits: dict
    :param debug: str
    :return traits: dict
    """

    # Read VIS image from Clowder
    vis_r = session.get(posixpath.join(url, "api/files", vis_id), stream=True)
    img_array = np.asarray(bytearray(vis_r.content), dtype="uint8")
    img = cv2.imdecode(img_array, -1)

    # Read the VIS top-view image mask for zoom = 1 from Clowder
    mask_r = session.get(posixpath.join(url, "api/files/57451b28e4b0efbe2dc3d4d5"), stream=True)
    mask_array = np.asarray(bytearray(mask_r.content), dtype="uint8")
    brass_mask = cv2.imdecode(mask_array, -1)

    # Read NIR image from Clowder
    nir_r = session.get(posixpath.join(url, "api/files", nir_id), stream=True)
    nir_array = np.asarray(bytearray(nir_r.content), dtype="uint8")
    nir = cv2.imdecode(nir_array, -1)
    nir_rgb = cv2.cvtColor(nir, cv2.COLOR_GRAY2BGR)

    [vis_traits, nir_traits] = process_tv_images_core(vis_id, img, nir_id, nir_rgb, nir, brass_mask, traits, debug)

    add_plantcv_metadata(session, url, vis_id, vis_traits)
    add_plantcv_metadata(session, url, nir_id, nir_traits)

    return traits

def process_tv_images_core(vis_id, vis_img, nir_id, nir_rgb, nir_cv2, brass_mask, traits, debug=None):
    device = 0

    # Convert RGB to HSV and extract the Saturation channel
    device, s = pcv.rgb2gray_hsv(vis_img, 's', device, debug)

    # Threshold the Saturation image
    device, s_thresh = pcv.binary_threshold(s, 75, 255, 'light', device, debug)

    # Median Filter
    device, s_mblur = pcv.median_blur(s_thresh, 5, device, debug)
    device, s_cnt = pcv.median_blur(s_thresh, 5, device, debug)

    # Fill small objects
    device, s_fill = pcv.fill(s_mblur, s_cnt, 150, device, debug)

    # Convert RGB to LAB and extract the Blue channel
    device, b = pcv.rgb2gray_lab(vis_img, 'b', device, debug)

    # Threshold the blue image
    device, b_thresh = pcv.binary_threshold(b, 138, 255, 'light', device, debug)
    device, b_cnt = pcv.binary_threshold(b, 138, 255, 'light', device, debug)

    # Fill small objects
    device, b_fill = pcv.fill(b_thresh, b_cnt, 100, device, debug)

    # Join the thresholded saturation and blue-yellow images
    device, bs = pcv.logical_and(s_fill, b_fill, device, debug)

    # Apply Mask (for vis images, mask_color=white)
    device, masked = pcv.apply_mask(vis_img, bs, 'white', device, debug)

    # Mask pesky brass piece
    device, brass_mask1 = pcv.rgb2gray_hsv(brass_mask, 'v', device, debug)
    device, brass_thresh = pcv.binary_threshold(brass_mask1, 0, 255, 'light', device, debug)
    device, brass_inv = pcv.invert(brass_thresh, device, debug)
    device, brass_masked = pcv.apply_mask(masked, brass_inv, 'white', device, debug)

    # Further mask soil and car
    device, masked_a = pcv.rgb2gray_lab(brass_masked, 'a', device, debug)
    device, soil_car1 = pcv.binary_threshold(masked_a, 128, 255, 'dark', device, debug)
    device, soil_car2 = pcv.binary_threshold(masked_a, 128, 255, 'light', device, debug)
    device, soil_car = pcv.logical_or(soil_car1, soil_car2, device, debug)
    device, soil_masked = pcv.apply_mask(brass_masked, soil_car, 'white', device, debug)

    # Convert RGB to LAB and extract the Green-Magenta and Blue-Yellow channels
    device, soil_a = pcv.rgb2gray_lab(soil_masked, 'a', device, debug)
    device, soil_b = pcv.rgb2gray_lab(soil_masked, 'b', device, debug)

    # Threshold the green-magenta and blue images
    device, soila_thresh = pcv.binary_threshold(soil_a, 124, 255, 'dark', device, debug)
    device, soilb_thresh = pcv.binary_threshold(soil_b, 148, 255, 'light', device, debug)

    # Join the thresholded saturation and blue-yellow images (OR)
    device, soil_ab = pcv.logical_or(soila_thresh, soilb_thresh, device, debug)
    device, soil_ab_cnt = pcv.logical_or(soila_thresh, soilb_thresh, device, debug)

    # Fill small objects
    device, soil_cnt = pcv.fill(soil_ab, soil_ab_cnt, 300, device, debug)

    # Apply mask (for vis images, mask_color=white)
    device, masked2 = pcv.apply_mask(soil_masked, soil_cnt, 'white', device, debug)

    # Identify objects
    device, id_objects, obj_hierarchy = pcv.find_objects(masked2, soil_cnt, device, debug)

    # Define ROI
    device, roi1, roi_hierarchy = pcv.define_roi(vis_img, 'rectangle', device, None, 'default', debug, True, 600, 450, -600,
                                                 -350)

    # Decide which objects to keep
    device, roi_objects, hierarchy3, kept_mask, obj_area = pcv.roi_objects(vis_img, 'partial', roi1, roi_hierarchy,
                                                                           id_objects, obj_hierarchy, device, debug)

    # Object combine kept objects
    device, obj, mask = pcv.object_composition(vis_img, roi_objects, hierarchy3, device, debug)

    # Find shape properties, output shape image (optional)
    device, shape_header, shape_data, shape_img = pcv.analyze_object(vis_img, vis_id, obj, mask, device, debug)

    # Determine color properties
    device, color_header, color_data, color_img = pcv.analyze_color(vis_img, vis_id, mask, 256, device, debug, None,
                                                                    'v', 'img', 300)

    # Output shape and color data
    vis_traits = {}
    for i in range(1, len(shape_header)):
        vis_traits[shape_header[i]] = shape_data[i]
    for i in range(2, len(color_header)):
        vis_traits[color_header[i]] = serialize_color_data(color_data[i])

    ############################# Use VIS image mask for NIR image#########################


    # Flip mask
    device, f_mask = pcv.flip(mask, "horizontal", device, debug)

    # Reize mask
    device, nmask = pcv.resize(f_mask, 0.116148, 0.116148, device, debug)

    # position, and crop mask
    device, newmask = pcv.crop_position_mask(nir_rgb, nmask, device, 15, 5, "top", "right", debug)

    # Identify objects
    device, nir_objects, nir_hierarchy = pcv.find_objects(nir_rgb, newmask, device, debug)

    # Object combine kept objects
    device, nir_combined, nir_combinedmask = pcv.object_composition(nir_rgb, nir_objects, nir_hierarchy, device, debug)

    ####################################### Analysis #############################################

    device, nhist_header, nhist_data, nir_imgs = pcv.analyze_NIR_intensity(nir_cv2, nir_id, nir_combinedmask, 256,
                                                                           device, False, debug)
    device, nshape_header, nshape_data, nir_shape = pcv.analyze_object(nir_cv2, nir_id, nir_combined, nir_combinedmask,
                                                                       device, debug)

    nir_traits = {}
    for i in range(1, len(nshape_header)):
        nir_traits[nshape_header[i]] = nshape_data[i]
    for i in range(2, len(nhist_header)):
        nir_traits[nhist_header[i]] = serialize_color_data(nhist_data[i])

    # Add data to traits table
    traits['tv_area'] = vis_traits['area']

    return [vis_traits, nir_traits]

# Process top-view images
###########################################
def add_plantcv_metadata(session, url, fileid, metadata):
    """Add PlantCV results as metadata to processed file in Clowder.

    Inputs:
    session  = requests session object
    url      = Clowder URL
    fileid   = the Clowder ID of an image
    metadata = trait dictionary output from PlantCV

    :param session: requests session object
    :param url: str
    :param fileid: str
    :param metadata: dict
    :return:
    """
    # print(json.dumps(metadata))
    r = session.post(posixpath.join(url, "api/files", fileid, "metadata"),
                     headers={"Content-Type": "application/json"}, data=json.dumps(metadata))

    # Was the upload successful?
    if r.status_code != 200:
        raise StandardError("Uploading metadata failed: Return value = {0}".format(r.status_code))


# Average trait
###########################################
def average_trait(list):
    total = sum(list)
    average = total / len(list)

    return average


# Upload file to a Clowder dataset
###########################################
def upload_file_to_clowder(session, url, file, dataset_id, metadata, dryrun=False):
    """Upload a file to a Clowder dataset.

    Args:
        session: http session
        url: Clowder URL
        file: File name and path
        dataset_id: Clowder dataset ID
        dryrun: Boolean. If true, no POST requests are made
    Returns:

    Raises:
        StandardError: HTTP POST return not 200
    """

    # Make sure file exists
    if os.path.exists(file):
        # Upload image file
        if dryrun is False:
            # Open file in binary mode
            f = open(file, 'rb')
            # Upload file to Clowder
            up_r = session.post(posixpath.join(url, "api/uploadToDataset", dataset_id),
                                files={"File" : f}, data=metadata)

            # Was the upload successful?
            if up_r.status_code != 200:
                raise StandardError("Uploading file failed: Return value = {0}".format(up_r.status_code))
    else:
        print("ERROR: Image file {0} does not exist".format(file), file=sys.stderr)
###########################################


if __name__ == '__main__':
    main()

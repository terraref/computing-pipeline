#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import argparse
import json
import cv2
import plantcv as pcv


def process_sv_images(vis_img, nir_img, traits, debug=False):
    """Process side-view images.

    Inputs:
    vis_img = An RGB image.
    nir_img = An NIR grayscale image.
    vis_traits  = traits table.
    nir_traits  = traits table.
    traits  = traits table.
    debug   = None, print, or plot. Print = save to file, Plot = print to screen.

    :param vis_img: str
    :param nir_img: str
    :param vis_traits: dict
    :param nir_traits: dict
    :param traits: dict
    :param debug: str
    :return: 
    """
    # Read VIS image
    img, path, filename = pcv.readimage(vis_img)

    # Pipeline step
    device = 0

    # Convert RGB to HSV and extract the Saturation channel
    device, s = pcv.rgb2gray_hsv(img, 's', device, debug)

    # Threshold the Saturation image
    device, s_thresh = pcv.binary_threshold(s, 36, 255, 'light', device, debug)

    # Median Filter
    device, s_mblur = pcv.median_blur(s_thresh, 5, device, debug)
    device, s_cnt = pcv.median_blur(s_thresh, 5, device, debug)

    # Fill small objects
    # device, s_fill = pcv.fill(s_mblur, s_cnt, 0, device, args.debug)

    # Convert RGB to LAB and extract the Blue channel
    device, b = pcv.rgb2gray_lab(img, 'b', device, debug)

    # Threshold the blue image
    device, b_thresh = pcv.binary_threshold(b, 137, 255, 'light', device, debug)
    device, b_cnt = pcv.binary_threshold(b, 137, 255, 'light', device, debug)

    # Fill small objects
    # device, b_fill = pcv.fill(b_thresh, b_cnt, 10, device, args.debug)

    # Join the thresholded saturation and blue-yellow images
    device, bs = pcv.logical_and(s_mblur, b_cnt, device, debug)

    # Apply Mask (for vis images, mask_color=white)
    device, masked = pcv.apply_mask(img, bs, 'white', device, debug)

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
    device, roi_objects, hierarchy3, kept_mask, obj_area = pcv.roi_objects(img, 'partial', roi1, roi_hierarchy,
                                                                           id_objects, obj_hierarchy, device,
                                                                           debug)

    # Object combine kept objects
    device, obj, mask = pcv.object_composition(img, roi_objects, hierarchy3, device, debug)

    ############## VIS Analysis ################
    # Find shape properties, output shape image (optional)
    device, shape_header, shape_data, shape_img = pcv.analyze_object(img, vis_img, obj, mask, device, debug)

    # Shape properties relative to user boundary line (optional)
    device, boundary_header, boundary_data, boundary_img1 = pcv.analyze_bound(img, vis_img, obj, mask, 384, device,
                                                                              debug)

    # Determine color properties: Histograms, Color Slices and
    # Pseudocolored Images, output color analyzed images (optional)
    device, color_header, color_data, color_img = pcv.analyze_color(img, vis_img, mask, 256, device, debug,
                                                                    None, 'v', 'img', 300)



    ############################# Use VIS image mask for NIR image#########################
    # Read NIR image
    nir, path1, filename1 = pcv.readimage(nir_img)
    nir2 = cv2.imread(nir_img, -1)

    # Flip mask
    device, f_mask = pcv.flip(mask, "vertical", device, debug)

    # Reize mask
    device, nmask = pcv.resize(f_mask, 0.1154905775, 0.1154905775, device, debug)

    # position, and crop mask
    device, newmask = pcv.crop_position_mask(nir, nmask, device, 30, 4, "top", "right", debug)

    # Identify objects
    device, nir_objects, nir_hierarchy = pcv.find_objects(nir, newmask, device, debug)

    # Object combine kept objects
    device, nir_combined, nir_combinedmask = pcv.object_composition(nir, nir_objects, nir_hierarchy, device, debug)

    ####################################### Analysis #############################################
    device, nhist_header, nhist_data, nir_imgs = pcv.analyze_NIR_intensity(nir2, filename1, nir_combinedmask, 256,
                                                                           device, False, debug)
    device, nshape_header, nshape_data, nir_shape = pcv.analyze_object(nir2, filename1, nir_combined, nir_combinedmask,
                                                                       device, debug)

    # Add data to traits table
    vis_traits = {}
    nir_traits = {}
    for i in range(1, len(shape_header)):
        vis_traits[shape_header[i]] = shape_data[i]
    for i in range(1, len(boundary_header)):
        vis_traits[boundary_header[i]] = boundary_data[i]
    for i in range(2, len(color_header)):
        vis_traits[color_header[i]] = serialize_color_data(color_data[i])

    for i in range(1, len(nshape_header)):
        nir_traits[nshape_header[i]] = nshape_data[i]
    for i in range(2, len(nhist_header)):
        nir_traits[nhist_header[i]] = serialize_color_data(nhist_data[i])
    
    traits['sv_area'].append(vis_traits['area'])
    traits['hull_area'].append(vis_traits['hull-area'])
    traits['solidity'].append(vis_traits['solidity'])
    traits['height'].append(vis_traits['height_above_bound'])
    traits['perimeter'].append(vis_traits['perimeter'])

    return [vis_traits, nir_traits]


def process_tv_images(vis_img, nir_img, traits, debug=False):
    """Process top-view images.

    Inputs:
    vis_img = An RGB image.
    nir_img = An NIR grayscale image.
    vis_traits  = traits table.
    nir_traits  = traits table.
    traits  = traits table.
    debug   = None, print, or plot. Print = save to file, Plot = print to screen.

    :param vis_img: str
    :param nir_img: str
    :param vis_traits: dict
    :param nir_traits: dict
    :param traits: dict
    :param debug: str
    :return: 
    """
    # Read image
    print ("starting processing...")
    img, path, filename = pcv.readimage(vis_img)
    brass_mask = cv2.imread('masks/mask_brass_tv_z1_L1.png')

    device = 0

    # Convert RGB to HSV and extract the Saturation channel
    device, s = pcv.rgb2gray_hsv(img, 's', device, debug)

    # Threshold the Saturation image
    device, s_thresh = pcv.binary_threshold(s, 75, 255, 'light', device, debug)

    # Median Filter
    device, s_mblur = pcv.median_blur(s_thresh, 5, device, debug)
    device, s_cnt = pcv.median_blur(s_thresh, 5, device, debug)

    # Fill small objects
    device, s_fill = pcv.fill(s_mblur, s_cnt, 150, device, debug)

    # Convert RGB to LAB and extract the Blue channel
    device, b = pcv.rgb2gray_lab(img, 'b', device, debug)

    # Threshold the blue image
    device, b_thresh = pcv.binary_threshold(b, 138, 255, 'light', device, debug)
    device, b_cnt = pcv.binary_threshold(b, 138, 255, 'light', device, debug)

    # Fill small objects
    device, b_fill = pcv.fill(b_thresh, b_cnt, 100, device, debug)

    # Join the thresholded saturation and blue-yellow images
    device, bs = pcv.logical_and(s_fill, b_fill, device, debug)

    # Apply Mask (for vis images, mask_color=white)
    device, masked = pcv.apply_mask(img, bs, 'white', device, debug)

    # Mask pesky brass piece
    #device, brass_mask1 = pcv.rgb2gray_hsv(brass_mask, 'v', device, debug)
    #device, brass_thresh = pcv.binary_threshold(brass_mask1, 0, 255, 'light', device, debug)
    #device, brass_inv = pcv.invert(brass_thresh, device, debug)
    #device, brass_masked = pcv.apply_mask(masked, brass_inv, 'white', device, debug)
    brass_masked = masked

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
    device, roi1, roi_hierarchy = pcv.define_roi(img, 'rectangle', device, None, 'default', debug, True, 600, 450, -600,
                                                 -350)

    # Decide which objects to keep
    device, roi_objects, hierarchy3, kept_mask, obj_area = pcv.roi_objects(img, 'partial', roi1, roi_hierarchy,
                                                                           id_objects, obj_hierarchy, device, debug)

    # Object combine kept objects
    device, obj, mask = pcv.object_composition(img, roi_objects, hierarchy3, device, debug)

    # Find shape properties, output shape image (optional)
    device, shape_header, shape_data, shape_img = pcv.analyze_object(img, vis_img, obj, mask, device, debug)

    # Determine color properties
    device, color_header, color_data, color_img = pcv.analyze_color(img, vis_img, mask, 256, device, debug, None,
                                                                    'v', 'img', 300)


    ############################# Use VIS image mask for NIR image#########################
    # Read NIR image
    nir, path1, filename1 = pcv.readimage(nir_img)
    nir2 = cv2.imread(nir_img, -1)

    # Flip mask
    device, f_mask = pcv.flip(mask, "horizontal", device, debug)

    # Reize mask
    device, nmask = pcv.resize(f_mask, 0.116148, 0.116148, device, debug)

    # position, and crop mask
    device, newmask = pcv.crop_position_mask(nir, nmask, device, 15, 5, "top", "right", debug)

    # Identify objects
    device, nir_objects, nir_hierarchy = pcv.find_objects(nir, newmask, device, debug)

    # Object combine kept objects
    device, nir_combined, nir_combinedmask = pcv.object_composition(nir, nir_objects, nir_hierarchy, device, debug)

    ####################################### Analysis #############################################

    device, nhist_header, nhist_data, nir_imgs = pcv.analyze_NIR_intensity(nir2, filename1, nir_combinedmask, 256,
                                                                           device, False, debug)
    device, nshape_header, nshape_data, nir_shape = pcv.analyze_object(nir2, filename1, nir_combined, nir_combinedmask,
                                                                       device, debug)
    print ("adding data...")
    # Add data to traits table
    vis_traits = {}
    nir_traits = {}
    for i in range(1, len(shape_header)):
        vis_traits[shape_header[i]] = shape_data[i]
    for i in range(2, len(color_header)):
        vis_traits[color_header[i]] = serialize_color_data(color_data[i])    

    for i in range(1, len(nshape_header)):
        nir_traits[nshape_header[i]] = nshape_data[i]
    for i in range(2, len(nhist_header)):
        nir_traits[nhist_header[i]] = serialize_color_data(nhist_data[i])

    traits['tv_area'] = vis_traits['area']
    
    return [vis_traits, nir_traits]
    print ("finshing the function...")



def serialize_color_data(list):

    newlist = [float(x) for x in list]

    return newlist

#!/usr/bin/env python

'''
Created on May 3, 2016
Author: Joshua Little
This script takes in a folder that contains the metadata associated with a particular
stereo pair (*_metadata.json) and the binary stereo images (*_left.bin and *_right.bin),
and outputs demosaiced .jpg files.
----------------------------------------------------------------------------------------
Usage:
python input_folder output_folder
where
input_folder    is the folder containing the metadata and binary stereo image inputs
output_folder   is the folder where the output .jpg files will be saved
----------------------------------------------------------------------------------------
Dependencies:
numpy, scipy, PIL (or Pillow)
'''

import sys, os.path, json
from glob import glob
from os.path import join
import numpy as np
from scipy.ndimage.filters import convolve
from PIL import Image

def main(in_dir, out_dir):
  if not os.path.isdir(in_dir):
    fail('Could not find input directory: ' + in_dir)
  if not os.path.isdir(out_dir):
    fail('Could not find output directory: ' + out_dir)
  
  metas, ims_left, ims_right = find_input_files(in_dir)
  
  for meta, im_left, im_right in zip(metas, ims_left, ims_right):
    metadata = load_json(join(in_dir, meta))
    left_shape = get_image_shape(metadata, 'left')
    right_shape = get_image_shape(metadata, 'right')
    process_image(im_left, left_shape, in_dir, out_dir)
    process_image(im_right, right_shape, in_dir, out_dir)

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

def process_image(im_name, shape, in_dir, out_dir):
  try:
    im = np.fromfile(join(in_dir, im_name), dtype='uint8').reshape(shape[::-1])
    im_color = demosaic(im)
    Image.fromarray(im_color).save(join(out_dir, im_name[:-4] + '.jpg'))
  except Exception as ex:
    fail('Error processing image "%s": %s' % (im_name, str(ex)))

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

def fail(reason):
  print >> sys.stderr, reason
  sys.exit(-1)

if __name__ == '__main__':
  if len(sys.argv) != 3:
    fail('Usage: python %s <input_folder> <output_folder>' % sys.argv[0])
  retcode = main(*sys.argv[1:3])

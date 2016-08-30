'''
Created on Aug 23, 2016

@author: Zongyang Li
'''

import os, json, sys
import multiprocessing
from glob import glob
import numpy as np
from PIL import Image
from math import cos, pi
from osgeo import gdal, osr

ZERO_ZERO = (33.0745,-111.97475)

tif_list_file = 'tif_list.txt'

TILE_FOLDER_NAME = 'flir_tiles'

def main(base_dir):
    
    print "Starting binary to image conversion..."
    full_day_convert(base_dir)
    print "Completed binary to image conversion..."
    

    ## Create VRT from every GeoTIFF
    #print "Starting VRT creation..."
    #createVrt(base_dir)
    #print "Completed VRT creation..."

    ## Generate tiles from VRT
    #print "Starting map tile creation..."
    #createMapTiles(base_dir,multiprocessing.cpu_count())
    #print "Completed map tile creation..."
    
    ## Generate google map html template
    #print "Starting google map html creation..."
    #generate_googlemaps(base_dir)
    #print "Completed google map html creation..."
    
    
    return

def full_day_convert(in_dir):

    try:
        os.remove(os.path.join(in_dir, tif_list_file)) # start from a fresh list of TIFFs for the day
    except OSError:
        pass
    
    list_dirs = os.walk(in_dir)
    
    for root, dirs, files in list_dirs:
        for d in dirs:
            full_path = os.path.join(in_dir, d)
            if not os.path.isdir(full_path):
                continue
            
            get_flir(full_path, in_dir)
    
    return

def createVrt(base_dir):
    # Create virtual tif for the files in this folder
    # Build a virtual TIF that combines all of the tifs in tif_file_list
    print "\tCreating virtual TIF..."
    try:
        vrtPath = os.path.join(base_dir,'virtualTif.vrt')
        tif_file = os.path.join(base_dir, tif_list_file)
        cmd = 'gdalbuildvrt -srcnodata "-99 -99 -99" -overwrite -input_file_list ' + tif_file +' ' + vrtPath
        os.system(cmd)
    except Exception as ex:
        fail("\tFailed to create virtual tif: " + str(ex))

def createMapTiles(base_dir,NUM_THREADS):
    # Create map tiles from the virtual tif
    # For now, just creating w/ local coordinate system. In the future, can make these actually georeferenced.
    print "\tCreating map tiles..."
    try:
        vrtPath = os.path.join(base_dir,'virtualTif.vrt')
        cmd = 'python gdal2tiles_parallel.py --processes=' + str(NUM_THREADS) + ' -l -n -e -f JPEG -z "18-28" -s EPSG:4326 ' + vrtPath + ' ' + os.path.join(base_dir,TILE_FOLDER_NAME)
        os.system(cmd)
    except Exception as ex:
        fail("Failed to generate map tiles: " + str(ex))


def get_flir(in_dir, base_dir):
    
    metafile, binfile = find_files(in_dir)
    if metafile == [] or binfile == [] :
        return
    
    metadata = lower_keys(load_json(metafile))
    
    center_position, scan_time, fov = parse_metadata(metadata)
    
    gps_bounds = get_bounding_box(center_position, fov)
    
    im_color = load_flir_data(binfile)
    
    out_png = binfile[:-3] + 'png'
    
    Image.fromarray(im_color).save(out_png)
    
    tif_path = binfile[:-3] + 'tif'
    create_geotiff(im_color, gps_bounds, tif_path, base_dir)
    
    return

def create_geotiff(np_arr, gps_bounds, out_file_path, base_dir):
    try:
        nrows,ncols = np.shape(np_arr)
        # gps_bounds: (lat_min, lat_max, lng_min, lng_max)
        xres = (gps_bounds[3] - gps_bounds[2])/float(ncols)
        yres = (gps_bounds[1] - gps_bounds[0])/float(nrows)
        geotransform = (gps_bounds[2],xres,0,gps_bounds[1],0,-yres) #(top left x, w-e pixel resolution, rotation (0 if North is up), top left y, rotation (0 if North is up), n-s pixel resolution)

        output_path = out_file_path

        output_raster = gdal.GetDriverByName('GTiff').Create(output_path, ncols, nrows, 3, gdal.GDT_Byte)

        output_raster.SetGeoTransform(geotransform) # specify coordinates
        srs = osr.SpatialReference() # establish coordinate encoding
        srs.ImportFromEPSG(4326) # specifically, google mercator
        output_raster.SetProjection( srs.ExportToWkt() ) # export coordinate system to file

        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        output_raster.GetRasterBand(1).WriteArray(np_arr.astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(1).FlushCache()
        output_raster.GetRasterBand(1).SetNoDataValue(-99)
        
        output_raster.GetRasterBand(2).WriteArray(np_arr.astype('uint8')) # write green channel to raster file
        output_raster.GetRasterBand(2).FlushCache()
        output_raster.GetRasterBand(2).SetNoDataValue(-99)

        output_raster.GetRasterBand(3).WriteArray(np_arr.astype('uint8')) # write blue channel to raster file
        output_raster.GetRasterBand(3).FlushCache()
        output_raster.GetRasterBand(3).SetNoDataValue(-99)


        # for test: once we've saved the image, make sure to append this path to our list of TIFs
        tif_file = os.path.join(base_dir, tif_list_file)
        f = open(tif_file,'a+')
        f.write(output_path + '\n')
    except Exception as ex:
        fail('Error creating GeoTIFF: ' + str(ex))

def load_flir_data(file_path):
    
    try:
        im = np.fromfile(file_path, np.dtype('<u2')).reshape([480, 640])
        Gmin = im.min()
        Gmax = im.max()
        At = ((im-Gmin) * 256 /(Gmax - Gmin))
        
        return At.astype('u1')
    except Exception as ex:
        fail('Error processing image "%s": %s' % (str(ex)))

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

def parse_metadata(metadata):
    
    try:
        gantry_meta = metadata['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
        gantry_x = gantry_meta["position x [m]"]
        gantry_y = gantry_meta["position y [m]"]
        gantry_z = gantry_meta["position z [m]"]
        
        scan_time = gantry_meta["time"]
        
        cam_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
        cam_x = cam_meta["location in camera box x [m]"]
        cam_y = cam_meta["location in camera box y [m]"]
        
        fov_x = cam_meta["field of view x [m]"]
        fov_y = cam_meta["field of view y [m]"]
        
        if "location in camera box z [m]" in cam_meta: # this may not be in older data
            cam_z = cam_meta["location in camera box z [m]"]
        else:
            cam_z = 0

    except KeyError as err:
        fail('Metadata file missing key: ' + err.args[0])
        
    position = [float(gantry_x), float(gantry_y), float(gantry_z)]
    center_position = [position[0]+float(cam_x), position[1]+float(cam_y), position[2]+float(cam_z)]
    fov = [float(fov_x), float(fov_y)]
    
    return center_position, scan_time, fov
    
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
    
def load_json(meta_path):
    try:
        with open(meta_path, 'r') as fin:
            return json.load(fin)
    except Exception as ex:
        fail('Corrupt metadata file, ' + str(ex))

def find_files(in_dir):
    json_suffix = os.path.join(in_dir, '*_metadata.json')
    jsons = glob(json_suffix)
    if len(jsons) == 0:
        fail('Could not find .json file')
        return [], []
        
        
    bin_suffix = os.path.join(in_dir, '*_ir.bin')
    bins = glob(bin_suffix)
    if len(bins) == 0:
        fail('Could not find .bin file')
        return [], []
    
    
    return jsons[0], bins[0]

def generate_googlemaps(base_dir):
        args = os.path.join(base_dir, TILE_FOLDER_NAME)

        s = """
            <!DOCTYPE html>
                <html>
                  <head>
                    <title>Map Create By Left Sensor</title>
                    <meta name="viewport" content="initial-scale=1.0">
                    <meta charset="utf-8">
                    <style>
                      html, body {
                        height: 100%%;
                        margin: 0;
                        padding: 0;
                      }
                      #map {
                        height: 100%%;
                      }
                    </style>
                  </head>
                  <body>
                    <div id="map"></div>
                    <script>
                      function initMap() {
                          var MyCenter = new google.maps.LatLng(33.0726220351,-111.974918861);
                  var map = new google.maps.Map(document.getElementById('map'), {
                    center: MyCenter,
                    zoom: 18,
                    streetViewControl: false,
                    mapTypeControlOptions: {
                      mapTypeIds: ['Terra']
                    }
                  });
                  
                
                
                  var terraMapType = new google.maps.ImageMapType({
                    getTileUrl: function(coord, zoom) {
                        var bound = Math.pow(2, zoom);
                        var y = bound-coord.y-1;
                       return '%s' +'/' + zoom + '/' + coord.x + '/' + y + '.jpg';
                    },
                    tileSize: new google.maps.Size(256, 256),
                    maxZoom: 28,
                    minZoom: 18,
                    radius: 1738000,
                    name: 'Terra'
                  });
                  
                  map.mapTypes.set('Terra', terraMapType);
                  map.setMapTypeId('Terra');
                }
                
                    </script>
                    <script src="https://maps.googleapis.com/maps/api/js?key=AIzaSyDJW9xwkAN3sfZE4FvGGLcgufJO9oInIHk&callback=initMap"async defer></script>
                  </body>
                </html>
            """ % args
        
        f = open(os.path.join(base_dir, 'opengooglemaps.html'), 'w')
        f.write(s)
        f.close()

        return s


def fail(reason):
    print >> sys.stderr, reason


if __name__ == "__main__":

    if len(sys.argv) != 2:
        fail('Usage: python %s <input_folder>' % sys.argv[0])
    retcode = main(sys.argv[1])

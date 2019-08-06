import os
from collections import OrderedDict

"""
Dictionary of count definitions for various sensors.

Types:
    timestamp:  count timestamp directories in each date directory
    psql:       count rows returned from specified postgres query
    regex:      count files within each date directory that match regex
Other fields:
    path:       path containing date directories for timestamp or regex counts
    regex:      regular expression to execute on date directory for regex counts
    query:      postgres query to execute for psql counts
    parent:     previous count definition for % generation (e.g. bin2tif's parent is stereoTop)
"""
uamac_root = "/home/clowder/sites/ua-mac/"
SENSOR_COUNT_DEFINITIONS = {

    "stereoTop": OrderedDict([
        # basic products
        ("stereoTop", {
            "path": os.path.join(uamac_root, 'raw_data/stereoTop/'),
            "type": 'timestamp'
        }),
        ("rgb_geotiff", {
            "path": os.path.join(uamac_root, 'Level_1/rgb_geotiff/'),
            "type": 'timestamp',
            "parent": "stereoTop",
            "extractor": "terra.stereo-rgb.bin2tif"}),
        ("rgb_nrmac", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_nrmac/'),
            "type": 'timestamp',
            "parent": "rgb_geotiff",
            "extractor": "terra.stereo-rgb.nrmac"}),
        ("rgb_mask", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_mask/'),
            "type": 'timestamp',
            "parent": "rgb_geotiff",
            "extractor": "terra.stereo-rgb.rgbmask"}),
        # plot products
        ("rgb_geotiff_plot", {
            "path": os.path.join(uamac_root, 'Level_1_Plots/rgb_geotiff/'),
            "type": 'plot'}),
        # rulechecker & fieldmosaic products
        ("ruledb_rgbff", {
            "type": "psql",
            "query_count": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs' and output->>'date'='%s';",
            "query_list": "select distinct file_path from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs' and output->>'date'='%s';",
            "parent": "rgb_geotiff",
            "extractor": "ncsa.rulechecker.terra"}),
        ("rgbff", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_fullfield/'),
            "type": 'regex',
            "regex": ".*_rgb.tif"}),
        ("ruledb_nrmacff", {
            "type": "psql",
            "query_count": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs NRMAC' and output->>'date'='%s';",
            "query_list": "select distinct file_path from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs NRMAC' and output->>'date'='%s';",
            # Parent count is still rgb_geotiff because we are putting NRMAC in same datasets as those
            "parent": "rgb_geotiff",
            "extractor": "ncsa.rulechecker.terra"}),
        ("nrmacff", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_fullfield/'),
            "type": 'regex',
            "regex": ".*_nrmac.tif"}),
        ("ruledb_maskff", {
            "type": "psql",
            "query_count": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs Masked' and output->>'date'='%s';",
            "query_list": "select distinct file_path from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs Masked' and output->>'date'='%s';",
            # Parent count is still rgb_geotiff because we are putting rgb_mask in same datasets as those
            "parent": "rgb_geotiff",
            "extractor": "ncsa.rulechecker.terra"}),
        ("maskff", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_fullfield/'),
            "type": 'regex',
            "regex": ".*_mask.tif",
            "dispname": "Full Field",
            "extractor": "terra.stereo-rgb.canopycover"}),
        # BETYdb traits
        ("rgb_canopycover", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_fullfield/'),
            "type": 'regex',
            "regex": '.*_canopycover_bety.csv',
            "parent": "maskff",
            "parent_replacer_check": ["_canopycover_bety.csv", ".tif"],
            "extractor": "terra.stereo-rgb.canopycover"})
    ]),

    "flirIrCamera": OrderedDict([
        # basic products
        ("flirIrCamera", {
            "path": os.path.join(uamac_root, 'raw_data/flirIrCamera/'),
            "type": 'timestamp'}),
        ("ir_geotiff", {
            "path": os.path.join(uamac_root, 'Level_1/ir_geotiff/'),
            "type": 'timestamp',
            "parent": "flirIrCamera",
            "extractor": "terra.multispectral.flir2tif"}),
        # plot products
        ("ir_geotiff_plot", {
            "path": os.path.join(uamac_root, 'Level_1_Plots/ir_geotiff/'),
            "type": 'plot'}),
        # rulechecker & fieldmosaic products
        ("ruledb_flirff", {
            "type": "psql",
            "query_count": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='Thermal IR GeoTIFFs' and output->>'date'='%s';",
            "query_list": "select distinct file_path from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='Thermal IR GeoTIFFs' and output->>'date'='%s';",
            "parent": "ir_geotiff",
            "extractor": "ncsa.rulechecker.terra"}),
        ("flirff", {
            "path": os.path.join(uamac_root, 'Level_2/ir_fullfield/'),
            "type": 'regex',
            "regex": ".*_thumb.tif",
            "dispname": "Full Field"}),
        # BETYdb traits
        ("ir_meantemp", {
            "path": os.path.join(uamac_root, 'Level_3/ir_meantemp/'),
            "type": 'regex',
            "regex": '.*_meantemp_bety.csv',
            "parent": "flirff",
            "parent_replacer_check": ["_meantemp_bety.csv", "_thumb.tif"],
            "extractor": "terra.multispectral.meantemp"})
    ]),

    "scanner3DTop": OrderedDict([
        # basic products
        ("scanner3DTop", {
            "path": os.path.join(uamac_root, 'Level_1/scanner3DTop/'),
            "type": 'timestamp'}),
        ("laser3d_las", {
            "path": os.path.join(uamac_root, 'Level_1/laser3d_las/'),
            "type": 'timestamp',
            "parent": "scanner3DTop",
            "extractor": "terra.3dscanner.ply2las"}),
        # plot products
        ("laser3d_las_plot", {
            "path": os.path.join(uamac_root, 'Level_1_Plots/laser3d_las/'),
            "type": 'plot'}),
        ("laser3d_canopyheight", {
            "path": os.path.join(uamac_root, 'Level_3/laser3d_canopyheight/'),
            "type": 'plot',
            "parent": "laser3d_las_plot",
            "extractor": "terra.3dscanner.las2height"})
    ]),

    "VNIR": OrderedDict([
        # basic products
        ("VNIR", {
            "path": os.path.join(uamac_root, 'raw_data/VNIR/'),
            "type": 'timestamp'}),
        ("vnir_netcdf", {
            "path": os.path.join(uamac_root, 'Level_1/vnir_netcdf/'),
            "type": 'timestamp',
            "parent": "VNIR",
            "extractor": "terra.hyperspectral"}),
        ("vnir_soil_masks", {
            "path": os.path.join(uamac_root, 'Level_1/vnir_soil_masks/'),
            "type": 'timestamp',
            "parent": "vnir_netcdf",
            "extractor": "terra.sunshade.soil_removal"})
    ]),

    "SWIR": OrderedDict([
        # basic products
        ("SWIR", {
            "path": os.path.join(uamac_root, 'raw_data/SWIR/'),
            "type": 'timestamp'}),
        ("swir_netcdf", {
            "path": os.path.join(uamac_root, 'Level_1/swir_netcdf/'),
            "type": 'timestamp',
            "parent": "SWIR",
            "extractor": "terra.hyperspectral"})
    ]),

    "ps2Top": OrderedDict([
        # basic products
        ("ps2Top", {
            "path": os.path.join(uamac_root, 'raw_data/ps2Top/'),
            "type": 'timestamp'}),
        ("ps2_png", {
            "path": os.path.join(uamac_root, 'Level_1/ps2_png/'),
            "type": 'timestamp',
            "parent": "ps2Top",
            "extractor": "terra.multispectral.psii2png"})
    ]),

    "EnvironmentLogger": OrderedDict([
        # basic products
        ("EnvironmentLogger", {
            "path": os.path.join(uamac_root, 'raw_data/EnvironmentLogger/'),
            "type": 'regex',
            "regex": ".*_environmentlogger.json",}),
        ("envlog2netcdf", {
            "path": os.path.join(uamac_root, 'Level_1/envlog_netcdf/'),
            "type": 'regex',
            "regex": "envlog_netcdf_.*.nc",
            "extractor": "terra.environmental.envlog2netcdf"})
        #,("envlog2netcdf_csv", {
        #    "path": os.path.join(uamac_root, 'Level_1/envlog_netcdf/'),
        #    "type": 'regex',
        #    "regex": "envlog_netcdf_.*_geo.csv",
        #    "extractor": "terra.environmental.envlog2netcdf"})
    ])
}

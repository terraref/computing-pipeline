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
            "query": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs' and output->>'date'='%s';",
            "parent": "rgb_geotiff",
            "extractor": "ncsa.rulechecker.terra"}),
        ("rgbff", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_fullfield/'),
            "type": 'regex',
            "regex": ".*_rgb_thumb.tif"}),
        ("ruledb_nrmacff", {
            "type": "psql",
            "query": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs NRMAC' and output->>'date'='%s';",
            "parent": "rgb_nrmac",
            "extractor": "ncsa.rulechecker.terra"}),
        ("nrmacff", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_fullfield/'),
            "type": 'regex',
            "regex": ".*_nrmac_thumb.tif"}),
        ("ruledb_maskff", {
            "type": "psql",
            "query": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='RGB GeoTIFFs Masked' and output->>'date'='%s';",
            "parent": "rgb_mask",
            "extractor": "ncsa.rulechecker.terra"}),
        ("maskff", {
            "path": os.path.join(uamac_root, 'Level_2/rgb_fullfield/'),
            "type": 'regex',
            "regex": ".*_mask_thumb.tif"}),
        # BETYdb traits
        ("rgb_canopycover", {
            "path": os.path.join(uamac_root, 'Level_3/rgb_canopycover/'),
            "type": 'regex',
            "regex": '.*_canopycover_bety.csv',
            "parent": "maskff"})
    ]),

    "flirIrCamera": OrderedDict([
        # basic products
        ("flirIrCamera", {
            "path": os.path.join(uamac_root, 'raw_data/flirIrCamera/'),
            "type": 'timestamp'}),
        ("ir_geotiff", {
            "path": os.path.join(uamac_root, 'Level_1/ir_geotiff/'),
            "type": 'timestamp',
            "parent": "flirIrCamera"}),
        # plot products
        ("ir_geotiff_plot", {
            "path": os.path.join(uamac_root, 'Level_1_Plots/ir_geotiff/'),
            "type": 'plot'}),
        # rulechecker & fieldmosaic products
        ("ruledb_flirff", {
            "type": "psql",
            "query": "select count(distinct file_path) from extractor_ids where output->>'rule'='Full Field' and output->>'sensor'='Thermal IR GeoTIFFs' and output->>'date'='%s';",
            "parent": "ir_geotiff"}),
        ("flirff", {
            "path": os.path.join(uamac_root, 'Level_2/ir_fullfield/'),
            "type": 'regex',
            "regex": ".*_thumb.tif"}),
        # BETYdb traits
        ("ir_meantemp", {
            "path": os.path.join(uamac_root, 'Level_3/ir_meantemp/'),
            "type": 'regex',
            "regex": '.*_meantemp_bety.csv',
            "parent": "flirff"})
    ]),

    "scanner3DTop": OrderedDict([
        # basic products
        ("scanner3DTop", {
            "path": os.path.join(uamac_root, 'Level_1/scanner3DTop/'),
            "type": 'timestamp'}),
        ("laser3d_las", {
            "path": os.path.join(uamac_root, 'Level_1/laser3d_las/'),
            "type": 'timestamp',
            "parent": "scanner3DTop"}),
        # plot products
        ("laser3d_las_plot", {
            "path": os.path.join(uamac_root, 'Level_1_Plots/laser3d_las/'),
            "type": 'plot'}),
        ("laser3d_canopyheight", {
            "path": os.path.join(uamac_root, 'Level_3/laser3d_canopyheight/'),
            "type": 'plot',
            "parent": "laser3d_las_plot"})
    ])
}

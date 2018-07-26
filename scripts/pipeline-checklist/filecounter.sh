#!/usr/bin/env bash

STEREO_TOP_DIR=/data/terraref/sites/ua-mac/raw_data/stereoTop/
RGB_GEOTIFF_DIR=/data/terraref/sites/ua-mac/Level_1/rgb_geotiff/
FLIR_IR_CAMERA_DIR=/data/terraref/sites/ua-mac/raw_data/flirIrCamera/
IR_GEOTIFF_DIR=/data/terraref/sites/ua-mac/Level_1/ir_geotiff/

function count_stereo_top {
        #ls $STEREO_TOP_DIR$1 | wc -l
        count=$(ls $STEREO_TOP_DIR$1 | wc -l)
        echo $count
}

function count_rgb_geotiff {
        #echo RGB Geotiff for day $1
        count=$(ls $RGB_GEOTIFF_DIR$1 | wc -l)
        echo $count
}


function count_flir_ir {
        #echo Flir Ir Camera for day $1
        count=$(ls $FLIR_IR_CAMERA_DIR$1 | wc -l)
        echo $count
}

function count_ir_geotiff {
        #echo Ir Geotiff for day $1
        count=$(ls $IR_GEOTIFF_DIR$1 | wc -l)
        echo $count

}

function count_files() {
        echo "$PATH_TO_DESKTOP"$1
        ls $PATH_TO_DESKTOP$1 | wc -l

}

stereo_top_count=$(count_stereo_top $1)
#echo Stereo top count for $1 is $stereo_top_count
rgb_geotiff_count=$(count_rgb_geotiff $1)
flir_ir_count=$(count_flir_ir $1)
ir_geotiff_count=$(count_ir_geotiff $1)

echo $1,$stereo_top_count,$rgb_geotiff_count,$flir_ir_count,$ir_geotiff_count
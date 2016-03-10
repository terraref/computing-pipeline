#!/bin/bash

# Purpose: Convert raw imagery from raster to netCDF and annotate with metadata

# if there's any problem regarding the path, try to add the following command:
# export PATH="$PATH:" where follows the directory to the terraref file

# Prerequisites:
# GDAL: sudo aptitude install gdal
# NCO: sudo aptitude install nco
# Python: both Python 2.X and 3.X are OK, but 3.X preferred
# netCDF4: A third-party library for Python, install them using sudo ports install netCDF4

# Usage:
# terraref.sh > ~/terraref.out 2>&1 &

# Convert raster to netCDF
gdal_translate -ot Float32 -of netCDF ${DATA}/terraref/data ${DATA}/terraref/data.nc

# Convert netCDF3 to netCDF4
ncks -O -4 ${DATA}/terraref/data.nc ${DATA}/terraref/data.nc4

# Combine 2D TR image data into single 3D variable
# fxm: Currently only works with NCO branch HMB-20160131-VLIST
# Once this branch is merged into master, next step will work with generic NCO
# Until then image is split into 926 variables, each the raster of one band
ncap2 -4 -v -O -S ${HOME}/computing-pipeline/scripts/terraref.nco ${DATA}/terraref/data.nc4 ${DATA}/terraref/data.nc4

# Add workflow-specific metadata
ncatted -O -a "Conventions,global,o,sng,CF-1.5" -a "Project,global,o,sng,TERRAREF" ${DATA}/terraref/data.nc4

# Parse JSON metadata (sensor location, instrument configuration)
python ${HOME}/computing-pipeline/scripts/JsonDealer.py ${DATA}/terraref/test.json ${DATA}/terraref/test.nc4

# Combine metadata with data
ncks -A ${DATA}/terraref/test.nc4 ${DATA}/terraref/data.nc4



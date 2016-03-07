#!/bin/bash

# Purpose: Convert raw imagery from raster to netCDF and annotate with metadata

# Prerequisites:
# GDAL: sudo aptitude install gdal
# NCO: sudo aptitude install nco
# Python:

# Usage:
# terraref.sh > ~/terraref.out 2>&1 &

# Convert raster to netCDF
gdal_translate -ot Float32 -of netCDF ${DATA}/terraref/data ${DATA}/terraref/data.nc

# Convert netCDF3 to netCDF4
ncks -O -4 ${DATA}/terraref/data.nc ${DATA}/terraref/data.nc4

# Combine 2D TR image data into single 3D variable
# fxm: Currently only works with HMB-20160131-VLIST branch of NCO
ncap2 -4 -v -O -S ${HOME}/computing-pipeline/scripts/terraref.nco ${DATA}/terraref/data.nc4 ${DATA}/terraref/data.nc4

# Add metadata
ncatted -O -a "Conventions,global,o,sng,CF-1.5" -a "Project,global,o,sng,TERRAREF" ${DATA}/terraref/data.nc4

# Parse JSON metadata
python JsonDealer.py test.json ${DATA}/terraref/test.nc4

# Combine metadata with data
ncks -A ${DATA}/terraref/test.nc ${DATA}/terraref/data.nc4

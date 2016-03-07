#!/bin/bash

# Prerequisites:
# GDAL: sudo aptitude install gdal
# NCO: sudo aptitude install nco
# Python: 

# Convert raster to netCDF
gdal_translate -ot Float32 -of netCDF ${DATA}/terraref/data ${DATA}/terraref/data.nc

# Convert netCDF3 to netCDF4
ncks -O -4 ${DATA}/terraref/data.nc ${DATA}/Desktop/terraref/data.nc4

# Add metadata
ncatted -O -a "Conventions,global,o,sng,CF-1.5" -a "Project,global,o,sng,TERRAREF" ${DATA}/terraref/data.nc4

# Parse JSON metadata
python JsonDealer.py test.json ${DATA}/terraref/test.nc4

# Combine metadata with data
ncks -A ${DATA}/terraref/test.nc ${DATA}/terraref/data.nc4

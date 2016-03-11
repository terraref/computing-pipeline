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
# terraref.sh $fl > ~/terraref.out 2>&1 &

# Set script name and run directory
drc_pwd=${PWD}
nco_version=$(ncks --version 2>&1 >/dev/null | grep NCO | awk '{print $5}')
spt_nm=$(basename ${0}) # [sng] Script name
spt_pid=$$ # [nbr] Script PID (process ID)

# Set fonts for legibility
fnt_nrm=`tput sgr0` # Normal
fnt_bld=`tput bold` # Bold
fnt_rvr=`tput smso` # Reverse

# Defaults for command-line options and some derived variables
dbg_lvl=0 # [nbr] Debugging level
in_fl='whiteReference' # [sng] Input file stub
out_fl='whiteReference.nc4' # [sng] Output file name

function fnc_usg_prn { # NB: dash supports fnc_nm (){} syntax, not function fnc_nm{} syntax
    # Print usage
    printf "\nComplete documentation for ${fnt_bld}${spt_nm}${fnt_nrm} at https://github.com/terraref/computing-pipeline\n\n"
    printf "${fnt_rvr}Basic usage:${fnt_nrm} ${fnt_bld}$spt_nm -i in_fl -o out_fl${fnt_nrm}\n\n"
    echo "${fnt_rvr}-d${fnt_nrm} ${fnt_bld}dbg_lvl${fnt_nrm}  Debugging level (default ${fnt_bld}${dbg_lvl}${fnt_nrm})"
    echo "${fnt_rvr}-i${fnt_nrm} ${fnt_bld}in_fl${fnt_nrm}    Input filename (default ${fnt_bld}${in_fl}${fnt_nrm})"
    echo "${fnt_rvr}-o${fnt_nrm} ${fnt_bld}out_fl${fnt_nrm}   Output-file (empty copies Input filename) (default ${fnt_bld}${out_fl}${fnt_nrm})"
    exit 1
} # end fnc_usg_prn()

 # Check argument number and complain accordingly
arg_nbr=$#
if [ ${arg_nbr} -eq 0 ]; then
  fnc_usg_prn
fi # !arg_nbr

# Parse command-line options:
# http://stackoverflow.com/questions/402377/using-getopts-in-bash-shell-script-to-get-long-and-short-command-line-options
# http://tuxtweaks.com/2014/05/bash-getopts
cmd_ln="${spt_nm} ${@}"
while getopts :d:i:o: OPT; do
    case ${OPT} in
	d) dbg_lvl=${OPTARG} ;; # Debugging level
	i) in_fl=${OPTARG} ;; # Input file
	o) out_fl=${OPTARG} ;; # Output file
	\?) # Unrecognized option
	    printf "\nERROR: Option ${fnt_bld}-$OPTARG${fnt_nrm} not allowed"
	    fnc_usg_prn ;;
    esac
done
shift $((OPTIND-1)) # Advance one argument

# Derived variables
if [ -n "${1}" ]; then
    in_fl=${1}
fi # ${1}
if [ -n "${2}" ]; then
    out_fl=${2}
fi # ${2}

# Print initial state
if [ ${dbg_lvl} -ge 2 ]; then
    printf "dbg: dbg_lvl  = ${dbg_lvl}\n"
    printf "dbg: in_fl    = ${in_fl}\n"
    printf "dbg: out_fl   = ${out_fl}\n"
fi # !dbg

# Convert raster to netCDF
# Raw data stored in ENVI hyperspectral image format in file "data" with accompanying header file "data.hdr"
# Header file documentation:
# http://www.exelisvis.com/docs/ENVIHeaderFiles.html
# Header file indicates raw data is ENVI type 4: single-precision float
# More optimal for 16-bit input data would be ENVI type 2 (NC_SHORT) or type 12 (NC_USHORT)
# This would save factor of two in raw data and could obviate packing (which is lossy quantization)
cmd_trn="gdal_translate -ot Float32 -of netCDF ${DATA}/terraref/${in_fl} ${DATA}/terraref/${in_fl}.nc"
if [ ${dbg_lvl} -ne 2 ]; then
    eval ${cmd_trn}
    if [ $? -ne 0 ]; then
	printf "${spt_nm}: ERROR Failed to translate raw data. Debug this:\n${cmd_trn}\n"
	exit 1
    fi # !err
fi # !dbg
if [ $? -ne 0 ]; then
    exit 1
fi # !err
	    
# Convert netCDF3 to netCDF4
ncks -O -4 ${DATA}/terraref/${in_fl}.nc ${DATA}/terraref/${in_fl}.nc4

# Combine 2D TR image data into single 3D variable
# fxm: Currently only works with NCO branch HMB-20160131-VLIST
# Once this branch is merged into master, next step will work with generic NCO
# Until then image is split into 926 variables, each the raster of one band
# fxm: currently this step is slow, and may need to be rewritten to dedicated routine
ncap2 -4 -v -O -S ${HOME}/computing-pipeline/scripts/terraref.nco ${DATA}/terraref/${in_fl}.nc4 ${DATA}/terraref/${in_fl}.nc4

# Workflow-specific metadata
ncatted -O -a "Conventions,global,o,sng,CF-1.5" -a "Project,global,o,sng,TERRAREF" ${DATA}/terraref/${in_fl}.nc4

# Parse JSON metadata (sensor location, instrument configuration)
python ${HOME}/computing-pipeline/scripts/JsonDealer.py ${DATA}/terraref/test.json ${DATA}/terraref/test.nc4

# Combine metadata with data
ncks -A ${DATA}/terraref/test.nc4 ${DATA}/terraref/${in_fl}.nc4

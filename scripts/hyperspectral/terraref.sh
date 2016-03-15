#!/bin/bash

# Purpose: Convert raw imagery from 2D floating-point raster to 3D compressed netCDF annotated with metadata

# Source: https://github.com/terraref/computing-pipeline/tree/master/scripts/hyperspectral/terraref.sh

# Prerequisites:
# GDAL: sudo aptitude install gdal
# NCO: sudo aptitude install nco
# Python: both Python 2.X and 3.X are OK, but 3.X preferred
# netCDF4: third-party library for Python

# In Anaconda:
# conda install netCDF4

# Configure paths at High-Performance Computer Centers (HPCCs) based on ${HOSTNAME}
if [ -z "${HOSTNAME}" ]; then
    if [ -f /bin/hostname ] && [ -x /bin/hostname ]; then
	export HOSTNAME=`/bin/hostname`
    elif [ -f /usr/bin/hostname ] && [ -x /usr/bin/hostname ]; then
	export HOSTNAME=`/usr/bin/hostname`
    fi # !hostname
fi # HOSTNAME
# Default input and output directory is ${DATA}
if [ -z "${DATA}" ]; then
    case "${HOSTNAME}" in 
	roger* ) DATA="/lustre/atlas/world-shared/cli115/${USER}" ; ;; # NCSA roger compute nodes named fxm, fxm GB/node
	* ) DATA='/tmp' ; ;; # Other
    esac # !HOSTNAME
fi # DATA
# Ensure batch jobs access correct 'mpirun' (or, on edison, 'aprun') command, netCDF library, and NCO executables and library:
case "${HOSTNAME}" in 
    roger* )
        export PATH='/ccs/home/zender/bin'\:${PATH}
	export LD_LIBRARY_PATH='/sw/redhat6/netcdf/4.3.3.1/rhel6.6_gcc4.8.2--with-dap+hdf4/lib:/sw/redhat6/szip/2.1/rhel6.6_gnu4.8.2/lib:/ccs/home/zender/lib'\:${LD_LIBRARY_PATH} ; ;;
esac # !HOSTNAME

# Test cases (for Charlie's machines)
# terraref.sh $fl > ~/terraref.out 2>&1 &

# Debugging and Benchmarking:
# terraref.sh -d 1 -i ${DATA}/terraref/whiteReference -o whiteReference.nc -O ~/rgr > ~/terraref.out 2>&1 &

# dbg_lvl: 0 = Quiet, print basic status during evaluation
#          1 = Print configuration, full commands, and status to output during evaluation
#          2 = As in dbg_lvl=1, but _do not evaluate commands_
#          3 = As in dbg_lvl=1, and pass debug level through to NCO/ncks

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
cln_flg='Yes' # [flg] Clean-up (remove) intermediate files before exiting
dbg_lvl=0 # [nbr] Debugging level
drc_in='' # [sng] Input file directory
drc_in_xmp='~/drc_in' # [sng] Input file directory for examples
drc_out="${drc_pwd}" # [sng] Output file directory
drc_out_xmp="~/drc_out" # [sng] Output file directory for examples
drc_tmp='' # [sng] Temporary file directory
gaa_sng="--gaa rgr_script=${spt_nm} --gaa rgr_hostname=${HOSTNAME} --gaa rgr_version=${nco_version}" # [sng] Global attributes to add
hdr_pad='1000' # [B] Pad at end of header section
in_fl='whiteReference' # [sng] Input file stub
in_xmp='data' # [sng] Input file for examples
fl_nbr=0 # [nbr] Number of files
job_nbr=2 # [nbr] Job simultaneity for parallelism
mpi_flg='No' # [sng] Parallelize over nodes
mtd_mk='Yes' # [sng] Process metadata
nd_nbr=1 # [nbr] Number of nodes
out_fl='whiteReference.nc4' # [sng] Output file name
out_xmp='data.nc4' # [sng] Output file for examples
nco_opt='-O --no_tmp_fl' # [sng] NCO defaults (e.g., '-O -6 -t 1')
nco_usr='' # [sng] NCO user-configurable options (e.g., '-D 1')
tmp_fl='terraref_tmp.nc' # [sng] Temporary output file
unq_sfx=".pid${spt_pid}.tmp" # [sng] Unique suffix

att_flg='Yes' # [sng] Add workflow-specific metadata
d23_flg='Yes' # [sng] Convert 2D->3D
jsn_flg='Yes' # [sng] Parse metadata from JSON to netCDF
mrg_flg='Yes' # [sng] Merge JSON metadata with data
n34_flg='Yes' # [sng] Convert netCDF3 to netCDF4
trn_flg='Yes' # [sng] Translate raw data to netCDF

function fnc_usg_prn { # NB: dash supports fnc_nm (){} syntax, not function fnc_nm{} syntax
    # Print usage
    printf "\nComplete documentation for ${fnt_bld}${spt_nm}${fnt_nrm} at https://github.com/terraref/computing-pipeline\n\n"
    printf "${fnt_rvr}Basic usage:${fnt_nrm} ${fnt_bld}$spt_nm -i in_fl -o out_fl${fnt_nrm}\n\n"
    echo "${fnt_rvr}-d${fnt_nrm} ${fnt_bld}dbg_lvl${fnt_nrm}  Debugging level (default ${fnt_bld}${dbg_lvl}${fnt_nrm})"
    echo "${fnt_rvr}-I${fnt_nrm} ${fnt_bld}drc_in${fnt_nrm}   Input directory (empty means none) (default ${fnt_bld}${drc_in}${fnt_nrm})"
    echo "${fnt_rvr}-i${fnt_nrm} ${fnt_bld}in_fl${fnt_nrm}    Input filename (default ${fnt_bld}${in_fl}${fnt_nrm})"
    echo "${fnt_rvr}-j${fnt_nrm} ${fnt_bld}job_nbr${fnt_nrm}  Job simultaneity for parallelism (default ${fnt_bld}${job_nbr}${fnt_nrm})"
    echo "${fnt_rvr}-n${fnt_nrm} ${fnt_bld}nco_opt${fnt_nrm}  NCO options (empty means none) (default ${fnt_bld}${nco_opt}${fnt_nrm})"
    echo "${fnt_rvr}-O${fnt_nrm} ${fnt_bld}drc_out${fnt_nrm}  Output directory (default ${fnt_bld}${drc_out}${fnt_nrm})"
    echo "${fnt_rvr}-o${fnt_nrm} ${fnt_bld}out_fl${fnt_nrm}   Output-file (empty copies Input filename) (default ${fnt_bld}${out_fl}${fnt_nrm})"
    echo "${fnt_rvr}-p${fnt_nrm} ${fnt_bld}par_typ${fnt_nrm}  Parallelism type (default ${fnt_bld}${par_typ}${fnt_nrm})"
    echo "${fnt_rvr}-t${fnt_nrm} ${fnt_bld}trn_flg${fnt_nrm}  Translate ENVI to netCDF with gdal (default ${fnt_bld}${trn_flg}${fnt_nrm})"
    echo "${fnt_rvr}-u${fnt_nrm} ${fnt_bld}unq_sfx${fnt_nrm}  Unique suffix (prevents intermediate files from sharing names) (default ${fnt_bld}${unq_sfx}${fnt_nrm})"
    printf "\n"
    printf "Examples: ${fnt_bld}$spt_nm -i ${in_xmp} -o ${out_xmp} ${fnt_nrm}\n"
    printf "          ${fnt_bld}$spt_nm -I ${drc_in_xmp} -O ${drc_out_xmp} ${fnt_nrm}\n"
    printf "          ${fnt_bld}ls SWNIR*nc | $spt_nm -O ${drc_out_xmp} ${fnt_nrm}\n"
    printf "CZ Debug: ${spt_nm} -i \${DATA}/terraref/whiteReference -O \${DATA}/terraref > ~/terraref.out 2>&1 &\n"
    printf "          ${spt_nm} -I \${DATA}/terraref -O \${DATA}/terraref > ~/terraref.out 2>&1 &\n"
    printf "          ${spt_nm} -I /projects/arpae/terraref/raw_data/lemnatec_field -O /projects/arpae/terraref/outputs/lemnatec_field > ~/terraref.out 2>&1 &\n"
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
while getopts :d:I:i:j:n:O:o:p:tU:u: OPT; do
    case ${OPT} in
	d) dbg_lvl=${OPTARG} ;; # Debugging level
	I) drc_in=${OPTARG} ;; # Input directory
	i) in_fl=${OPTARG} ;; # Input file
	j) job_usr=${OPTARG} ;; # Job simultaneity
	n) nco_usr=${OPTARG} ;; # NCO options
	O) drc_usr=${OPTARG} ;; # Output directory
	o) out_fl=${OPTARG} ;; # Output file
	p) par_typ=${OPTARG} ;; # Parallelism type
	t) trn_flg='No' ;; # Translate flag
	U) tmp_usr=${OPTARG} ;; # Temporary directory
	u) unq_usr=${OPTARG} ;; # Unique suffix
	\?) # Unrecognized option
	    printf "\nERROR: Option ${fnt_bld}-$OPTARG${fnt_nrm} not allowed"
	    fnc_usg_prn ;;
    esac
done
shift $((OPTIND-1)) # Advance one argument

# Positional arguments remaining, if any, correspond to input and output files
if [ -n "${1}" ]; then
    in_fl=${1}
fi # ${1}
if [ -n "${2}" ]; then
    out_fl=${2}
fi # ${2}

# Derived variables
if [ -n "${drc_usr}" ]; then
    drc_out="${drc_usr}"
else
    if [ -n "${out_fl}" ]; then
	drc_out="$(dirname ${out_fl})"
    fi # !out_fl
fi # !drc_usr
if [ -n "${tmp_usr}" ]; then
	drc_tmp=${tmp_usr}
else
	drc_tmp=${drc_out}
fi # !out_fl
att_fl="${drc_tmp}/terraref_tmp_att.nc" # [sng] ncatted file
d23_fl="${drc_tmp}/terraref_tmp_d23.nc" # [sng] 2D->3D file
jsn_fl="${drc_tmp}/terraref_tmp_jsn.nc" # [sng] JSON file
mrg_fl="${drc_tmp}/terraref_tmp_mrg.nc" # [sng] Merge file
n34_fl="${drc_tmp}/terraref_tmp_n34.nc" # [sng] netCDF3->netCDF4 file
tmp_fl="${drc_tmp}/${tmp_fl}" # [sng] Temporary output file
trn_fl="${drc_tmp}/terraref_tmp_trn.nc" # [sng] Translate file

if [ -n "${unq_usr}" ]; then
    if [ "${unq_usr}" = 'noclean' ]; then
	cln_flg='No'
    else
	if [ "${unq_usr}" != 'none' ] && [ "${unq_usr}" != 'nil' ]; then
	    unq_sfx="${unq_usr}"
	else # !unq_usr
	    unq_sfx=""
	fi # !unq_usr
    fi # !unq_usr
fi # !unq_sfx
att_fl=${att_fl}${unq_sfx}
d23_fl=${d23_fl}${unq_sfx}
jsn_fl=${jsn_fl}${unq_sfx}
mrg_fl=${mrg_fl}${unq_sfx}
n34_fl=${n34_fl}${unq_sfx}
tmp_fl=${tmp_fl}${unq_sfx}
trn_fl=${trn_fl}${unq_sfx}

if [ -z "${drc_in}" ]; then
    drc_in="${drc_pwd}"
else # !drc_in
    drc_in_usr_flg='Yes'
fi # !drc_in
if [ -n "${job_usr}" ]; then 
    job_nbr="${job_usr}"
fi # !job_usr
if [ ${dbg_lvl} -ge 2 ]; then
    nco_opt="-D ${dbg_lvl} ${nco_opt}"
fi # !dbg_lvl
if [ -n "${nco_usr}" ]; then 
    nco_opt="${nco_usr} ${nco_opt}"
fi # !var_lst
if [ -n "${gaa_sng}" ]; then 
    nco_opt="${nco_opt} ${gaa_sng}"
fi # !var_lst
if [ -n "${hdr_pad}" ]; then 
    nco_opt="${nco_opt} --hdr_pad=${hdr_pad}"
fi # !hdr_pad
if [ -n "${out_fl}" ]; then 
    out_usr_flg='Yes'
fi # !out_fl
if [ -n "${par_typ}" ]; then
    if [ "${par_typ}" != 'bck' ] && [ "${par_typ}" != 'mpi' ] && [ "${par_typ}" != 'nil' ]; then 
	    echo "ERROR: Invalid -p par_typ option = ${par_typ}"
	    echo "HINT: Valid par_typ arguments are 'bck', 'mpi', and 'nil'"
	    exit 1
    fi # !par_typ
fi # !par_typ
if [ "${par_typ}" = 'bck' ]; then 
    par_opt=' &'
elif [ "${par_typ}" = 'mpi' ]; then 
    mpi_flg='Yes'
    par_opt=' &'
fi # !par_typ

# Parse metadata arguments before in_fl arguments so we know whether this could be a metadata-only invocation
if [ -n "${in_fl}" ]; then
    # Single file argument
    fl_in[${fl_nbr}]=${in_fl}
    let fl_nbr=${fl_nbr}+1
else # !in_fl
    # Detecting input on stdin:
    # http://stackoverflow.com/questions/2456750/detect-presence-of-stdin-contents-in-shell-script
    # ls ${DATA}/ne30/raw/famipc5*1979*.nc | ncremap -D 1 -m ${DATA}/maps/map_ne30np4_to_fv129x256_aave.20150901.nc -O ~/rgr
    if [ -t 0 ]; then 
	if [ "${drc_in_usr_flg}" = 'Yes' ]; then
	    for fl in "${drc_in}"/*.hdr ; do
		if [ -f "${fl}" ]; then
		    fl_in[${fl_nbr}]=${fl}
		    let fl_nbr=${fl_nbr}+1
		fi # !file
	    done
	else # !drc_in
	    if [ "${mtd_mk}" != 'Yes' ]; then 
		echo "ERROR: Must specify input file with -i or with stdin"
		echo "HINT: Pipe file list to script via stdin with, e.g., 'ls *.hdr | ${spt_nm}'"
		exit 1
	    fi # !mtd_mk
	fi # !drc_in
    else
	# Input awaits on unit 0, i.e., on stdin
	while read -r line; do # NeR05 p. 179
	    fl_in[${fl_nbr}]=${line}
	    let fl_nbr=${fl_nbr}+1
	done < /dev/stdin
    fi # stdin
fi # !in_fl

if [ "${mpi_flg}" = 'Yes' ]; then
    if [ -n "${COBALT_NODEFILE}" ]; then 
	nd_fl="${COBALT_NODEFILE}"
    elif [ -n "${PBS_NODEFILE}" ]; then 
	nd_fl="${PBS_NODEFILE}"
    elif [ -n "${SLURM_NODELIST}" ]; then 
	nd_fl="${SLURM_NODELIST}"
    fi # !PBS
    if [ -n "${nd_fl}" ]; then 
	# NB: nodes are 0-based, e.g., [0..11]
	nd_idx=0
	for nd in `cat ${nd_fl} | uniq` ; do
	    nd_nm[${nd_idx}]=${nd}
	    let nd_idx=${nd_idx}+1
	done # !nd
	nd_nbr=${#nd_nm[@]}
	for ((fl_idx=0;fl_idx<fl_nbr;fl_idx++)); do
	    case "${HOSTNAME}" in 
		cori* | edison* | nid* )
		    # NB: NERSC staff says srun automatically assigns to unique nodes even without "-L $node" argument?
		    cmd_mpi[${fl_idx}]="srun -L ${nd_nm[$((${fl_idx} % ${nd_nbr}))]} -n 1" ; ;; # NERSC
		hopper* )
		    # NB: NERSC migrated from aprun to srun in 201601. Hopper commands will soon be deprecated.
		    cmd_mpi[${fl_idx}]="aprun -L ${nd_nm[$((${fl_idx} % ${nd_nbr}))]} -n 1" ; ;; # NERSC
		* )
		    cmd_mpi[${fl_idx}]="mpirun -H ${nd_nm[$((${fl_idx} % ${nd_nbr}))]} -npernode 1 -n 1" ; ;; # Other
	    esac # !HOSTNAME
	done # !fl_idx
    else # ! pbs
	mpi_flg='No'
	for ((fl_idx=0;fl_idx<fl_nbr;fl_idx++)); do
	    cmd_mpi[${fl_idx}]=""
	done # !fl_idx
    fi # !pbs
    if [ -z "${job_usr}" ]; then 
	job_nbr=${nd_nbr}
    fi # !job_usr
    if [ -z "${thr_usr}" ]; then 
	if [ -n "${PBS_NUM_PPN}" ]; then
#	NB: use export OMP_NUM_THREADS when thr_nbr > 8
#	thr_nbr=${PBS_NUM_PPN}
	    thr_nbr=$((PBS_NUM_PPN > 8 ? 8 : PBS_NUM_PPN))
	fi # !pbs
    fi # !thr_usr
fi # !mpi

# Print initial state
if [ ${dbg_lvl} -ge 2 ]; then
    printf "dbg: cln_flg  = ${cln_flg}\n"
    printf "dbg: dbg_lvl  = ${dbg_lvl}\n"
    printf "dbg: drc_in   = ${drc_in}\n"
    printf "dbg: drc_out  = ${drc_out}\n"
    printf "dbg: drc_tmp  = ${drc_tmp}\n"
    printf "dbg: gaa_sng  = ${gaa_sng}\n"
    printf "dbg: hdr_pad  = ${hdr_pad}\n"
    printf "dbg: job_nbr  = ${job_nbr}\n"
    printf "dbg: in_fl    = ${in_fl}\n"
    printf "dbg: mpi_flg  = ${mpi_flg}\n"
    printf "dbg: nco_opt  = ${nco_opt}\n"
    printf "dbg: nd_nbr   = ${nd_nbr}\n"
    printf "dbg: out_fl   = ${out_fl}\n"
    printf "dbg: par_typ  = ${par_typ}\n"
    printf "dbg: spt_pid  = ${spt_pid}\n"
    printf "dbg: unq_sfx  = ${unq_sfx}\n"
    printf "Asked to process ${fl_nbr} files:\n"
    for ((fl_idx=0;fl_idx<${fl_nbr};fl_idx++)); do
	printf "${fl_in[${fl_idx}]}\n"
    done # !fl_idx
fi # !dbg
if [ ${dbg_lvl} -ge 2 ]; then
    if [ ${mpi_flg} = 'Yes' ]; then
	for ((nd_idx=0;nd_idx<${nd_nbr};nd_idx++)); do
	    printf "dbg: nd_nm[${nd_idx}] = ${nd_nm[${nd_idx}]}\n"
	done # !nd
    fi # !mpi
fi # !dbg

# Create output directory
mkdir -p ${drc_out}
mkdir -p ${drc_tmp}

# Human-readable summary
if [ ${dbg_lvl} -ge 1 ]; then
    printf "Terraref data pipeline invoked with command:\n"
    echo "${cmd_ln}"
fi # !dbg
date_srt=$(date +"%s")

# Begin loop over input files
idx_srt=0
let idx_end=$((job_nbr-1))
for ((fl_idx=0;fl_idx<${fl_nbr};fl_idx++)); do
    in_fl=${fl_in[${fl_idx}]}
    if [ "$(basename ${in_fl})" = "${in_fl}" ]; then
	in_fl="${drc_pwd}/${in_fl}"
    fi # !basename
    idx_prn=`printf "%02d" ${fl_idx}`
    printf "Input #${idx_prn}: ${in_fl}\n"
    if [ "${out_usr_flg}" = 'Yes' ]; then 
	if [ ${fl_nbr} -ge 2 ]; then 
	    echo "ERROR: Single output filename specified with -o for multiple input files"
	    echo "HINT: For multiple input files use -O option to specify output directory and do not use -o option. Output files will have same name as input files, but will be in different directory."
	    exit 1
	fi # !fl_nbr
	if [ -n "${drc_usr}" ]; then
	    out_fl="${drc_out}/${out_fl}"
	fi # !drc_usr
    else # !out_usr_flg
	out_fl="${drc_out}/$(basename ${in_fl})"
    fi # !out_fl
    if [ "${in_fl}" = "${out_fl}" ]; then
	echo "ERROR: Input file = Output file = ${in_fl}"
	echo "HINT: To prevent inadvertent data loss, ${spt_nm} insists that Input file and Output filenames differ"
	exit 1
    fi # !basename

    # Convert raster to netCDF
    # Raw data stored in ENVI hyperspectral image format in file "data" with accompanying header file "data.hdr"
    # Header file documentation:
    # http://www.exelisvis.com/docs/ENVIHeaderFiles.html
    # Header file indicates raw data is ENVI type 4: single-precision float
    # More optimal for 16-bit input data would be ENVI type 2 (NC_SHORT) or type 12 (NC_USHORT)
    # This would save factor of two in raw data and could obviate packing (which is lossy quantization)
    if [ "${trn_flg}" = 'Yes' ]; then
	printf "trn(in)  : ${in_fl}\n"
	printf "trn(out) : ${trn_fl}\n"
	cmd_trn[${fl_idx}]="gdal_translate -ot Float32 -of netCDF ${in_fl} ${trn_fl}"
	hst_att="`date`: ${cmd_ln};${cmd_trn[${fl_idx}]}"
	in_fl=${trn_fl}
	if [ ${dbg_lvl} -ge 1 ]; then
	    echo ${cmd_trn[${fl_idx}]}
	fi # !dbg
	if [ ${dbg_lvl} -ne 2 ]; then
	    eval ${cmd_trn[${fl_idx}]}
	    if [ $? -ne 0 ] || [ ! -f ${trn_fl} ]; then
		printf "${spt_nm}: ERROR Failed to translate raw data. Debug this:\n${cmd_trn[${fl_idx}]}\n"
		exit 1
	    fi # !err
	fi # !dbg
    fi # !trn_flg
    
    # Add workflow-specific metadata
    if [ "${att_flg}" = 'Yes' ]; then
	printf "att(in)  : ${in_fl}\n"
	printf "att(out) : ${att_fl}\n"
	cmd_att[${fl_idx}]="ncatted -O ${gaa_sng} -a \"Conventions,global,o,c,CF-1.5\" -a \"Project,global,o,c,TERRAREF\" --gaa history='${hst_att}' ${in_fl} ${att_fl}"
	in_fl=${att_fl}
	if [ ${dbg_lvl} -ge 1 ]; then
	    echo ${cmd_att[${fl_idx}]}
	fi # !dbg
	if [ ${dbg_lvl} -ne 2 ]; then
	    eval ${cmd_att[${fl_idx}]}
	    if [ $? -ne 0 ] || [ ! -f ${att_fl} ]; then
		printf "${spt_nm}: ERROR Failed to annotate metadata with ncatted. Debug this:\n${cmd_att[${fl_idx}]}\n"
		exit 1
	    fi # !err
	fi # !dbg
    fi # !att_flg
    
    # Parse metadata from JSON to netCDF (sensor location, instrument configuration)
    if [ "${jsn_flg}" = 'Yes' ]; then
	printf "jsn(in)  : ${in_fl}\n"
	printf "jsn(out) : ${jsn_fl}\n"
	# fxm: Verify naming convention for .json files
	in_jsn="${fl_in[${fl_idx}]}.json" # [sng] JSON input file
	cmd_jsn[${fl_idx}]="python ${HOME}/computing-pipeline/scripts/JsonDealer.py ${in_jsn} ${jsn_fl}"
	in_fl=${jsn_fl}
	if [ ${dbg_lvl} -ge 1 ]; then
	    echo ${cmd_jsn[${fl_idx}]}
	fi # !dbg
	if [ ${dbg_lvl} -ne 2 ]; then
	    eval ${cmd_jsn[${fl_idx}]}
	    if [ $? -ne 0 ] || [ ! -f ${jsn_fl} ]; then
		printf "${spt_nm}: ERROR Failed to parse JSON metadata. Debug this:\n${cmd_jsn[${fl_idx}]}\n"
		exit 1
	    fi # !err
	fi # !dbg
    fi # !jsn_flg

    # Block 5: Convert 2D->3D
    # Combine 2D TR image data into single 3D variable
    # fxm: Currently only works with NCO branch HMB-20160131-VLIST
    # Once this branch is merged into master, next step will work with generic NCO
    # Until then image is split into 926 variables, each the raster of one band
    # fxm: currently this step is slow, and may need to be rewritten to dedicated routine
    printf "2D  : ${in_fl}\n"
    printf "3D  : ${d23_fl}\n"
    cmd_d23[${fl_idx}]="${cmd_mpi[${fl_idx}]} ncap2 -4 -v -O -S ${HOME}/terraref/computing-pipeline/scripts/terraref.nco ${in_fl} ${d23_fl}"
    in_fl=${d23_fl}
    
    # Block 5 Loop 2: Execute and/or echo commands
    if [ ${dbg_lvl} -ge 1 ]; then
	echo ${cmd_d23[${fl_idx}]}
    fi # !dbg
    if [ ${dbg_lvl} -ne 2 ]; then
	if [ -z "${par_opt}" ]; then
	    eval ${cmd_d23[${fl_idx}]}
	    if [ $? -ne 0 ]; then
		printf "${spt_nm}: ERROR Failed to convert 2D->3D. cmd_d23[${fl_idx}] failed. Debug this:\n${cmd_d23[${fl_idx}]}\n"
		exit 1
	    fi # !err
	else # !par_typ
	    eval ${cmd_d23[${fl_idx}]} ${par_opt}
	    d23_pid[${fl_idx}]=$!
	fi # !par_typ
    fi # !dbg

    # Block 6: Wait
    # Parallel processing (both Background and MPI) spawn simultaneous processes in batches of ${job_nbr}
    # Once ${job_nbr} jobs are running, wait() for all to finish before issuing another batch
    if [ -n "${par_opt}" ]; then
	let bch_idx=$((fl_idx / job_nbr))
	let bch_flg=$(((fl_idx+1) % job_nbr))
	if [ ${bch_flg} -eq 0 ]; then
	    if [ ${dbg_lvl} -ge 1 ]; then
		printf "${spt_nm}: Waiting for batch ${bch_idx} to finish at fl_idx = ${fl_idx}...\n"
	    fi # !dbg
	    for ((pid_idx=${idx_srt};pid_idx<=${idx_end};pid_idx++)); do
		wait ${d23_pid[${pid_idx}]}
		if [ $? -ne 0 ]; then
		    printf "${spt_nm}: ERROR Failed to convert 2D->3D. cmd_d23[${pid_idx}] failed. Debug this:\n${cmd_d23[${pid_idx}]}\n"
		    exit 1
		fi # !err
	    done # !pid_idx
	    let idx_srt=$((idx_srt + job_nbr))
	    let idx_end=$((idx_end + job_nbr))
	fi # !bch_flg
    fi # !par_typ
    
done # !fl_idx

# Parallel mode will often exit loop after a partial batch, wait() for remaining jobs to finish
if [ -n "${par_opt}" ]; then
    let bch_flg=$((fl_nbr % job_nbr))
    if [ ${bch_flg} -ne 0 ]; then
	let bch_idx=$((bch_idx+1))
	printf "${spt_nm}: Waiting for (partial) batch ${bch_idx} to finish...\n"
	for ((pid_idx=${idx_srt};pid_idx<${fl_nbr};pid_idx++)); do
	    wait ${d23_pid[${pid_idx}]}
	    if [ $? -ne 0 ]; then
		printf "${spt_nm}: ERROR Failed to convert 2D->3D. cmd_d23[${pid_idx}] failed. Debug this:\n${cmd_d23[${pid_idx}]}\n"
		exit 1
	    fi # !err
	done # !pid_idx
    fi # !bch_flg
fi # !par_typ

# Final loop, in serial mode, to finish processing
for ((fl_idx=0;fl_idx<${fl_nbr};fl_idx++)); do
    # fxm: remove this redundant chunk of code to determine in_fl, out_fl
    in_fl=${fl_in[${fl_idx}]}
    if [ "$(basename ${in_fl})" = "${in_fl}" ]; then
	in_fl="${drc_pwd}/${in_fl}"
    fi # !basename
    idx_prn=`printf "%02d" ${fl_idx}`
    printf "Input #${idx_prn}: ${in_fl}\n"
    if [ "${out_usr_flg}" = 'Yes' ]; then 
	if [ ${fl_nbr} -ge 2 ]; then 
	    echo "ERROR: Single output filename specified with -o for multiple input files"
	    echo "HINT: For multiple input files use -O option to specify output directory and do not use -o option. Output files will have same name as input files, but will be in different directory."
	    exit 1
	fi # !fl_nbr
	if [ -n "${drc_usr}" ]; then
	    out_fl="${drc_out}/${out_fl}"
	fi # !drc_usr
    else # !out_usr_flg
	out_fl="${drc_out}/$(basename ${in_fl})"
    fi # !out_fl
    if [ "${in_fl}" = "${out_fl}" ]; then
	echo "ERROR: Input file = Output file = ${in_fl}"
	echo "HINT: To prevent inadvertent data loss, ${spt_nm} insists that Input file and Output filenames differ"
	exit 1
    fi # !basename

    # Convert netCDF3 to netCDF4
    if [ "${n34_flg}" = 'Yes' ]; then
	printf "n34(in)  : ${in_fl}\n"
	printf "n34(out) : ${n34_fl}\n"
	cmd_n34[${fl_idx}]="ncks -O -4 ${in_fl} ${n34_fl}"
	in_fl=${n34_fl}
	if [ ${dbg_lvl} -ge 1 ]; then
	    echo ${cmd_n34[${fl_idx}]}
	fi # !dbg
	if [ ${dbg_lvl} -ne 2 ]; then
	    eval ${cmd_n34[${fl_idx}]}
	    if [ $? -ne 0 ] || [ ! -f ${n34_fl} ]; then
		printf "${spt_nm}: ERROR Failed to convert netCDF3 to netCDF4. Debug this:\n${cmd_n34[${fl_idx}]}\n"
		exit 1
	    fi # !err
	fi # !dbg
    fi # !n34_flg
    
    # Merge JSON metadata with data
    if [ "${mrg_flg}" = 'Yes' ]; then
	printf "mrg(in)  : ${in_fl}\n"
	printf "mrg(out) : ${mrg_fl}\n"
	cmd_mrg[${fl_idx}]="ncks -A ${DATA}/terraref/test.nc4 ${in_fl} ${mrg_fl}"
	in_fl=${mrg_fl}
	if [ ${dbg_lvl} -ge 1 ]; then
	    echo ${cmd_mrg[${fl_idx}]}
	fi # !dbg
	if [ ${dbg_lvl} -ne 2 ]; then
	    eval ${cmd_mrg[${fl_idx}]}
	    if [ $? -ne 0 ] || [ ! -f ${mrg_fl} ]; then
		printf "${spt_nm}: ERROR Failed to merge JSON metadata with data file. Debug this:\n${cmd_mrg[${fl_idx}]}\n"
		exit 1
	    fi # !err
	fi # !dbg
    fi # !mrg_flg

done # !fl_idx

if [ "${cln_flg}" = 'Yes' ]; then
    printf "Cleaning-up intermediate files...\n"
    /bin/rm -f ${att_fl} ${d23_fl} ${jsn_fl} ${mrg_fl} ${n34_fl} ${tmp_fl} ${trn_fl}
fi # !cln_flg

date_end=$(date +"%s")
if [ ${fl_nbr} -eq 0 ]; then
    printf "Completed pipeline at `date`.\n"
else # !fl_nbr
    echo "Quick plots of results from last processed file:"
    echo "ncview  ${out_fl} &"
    echo "panoply ${out_fl} &"
fi # !fl_nbr
date_dff=$((date_end-date_srt))
echo "Elapsed time $((date_dff/60))m$((date_dff % 60))s"

exit 0

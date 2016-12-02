# Purpose: Reduce raw calibration data (~10 GB) to exposures (~10 kB) used by hyperspectral_calibration.nco
# Script is run off-line, usually on CSZ's machines, although that could be generalized
# Input names and formats provided by Lemnatec are non-standard (e.g., 7Z)
# Hence this script will need to change for every generation of white and dark references
# Output files contain xps_img_[drk/wht] variables in one file per exposure time

# Source: https://github.com/terraref/computing-pipeline/tree/master/scripts/hyperspectral/hyperspectral_calibration_reduction.sh

# Directory for hyperspectral calibration output files
drc_clb=${DATA}/terraref/clb

# White reference, single pixel (px1), created by Solmaz (NB: pre-compensated for dark counts)
# "Single pixel" (px1) values, can be compared to the difference (white-dark) of the following area-averaged calibrations
for drc in 2016_10_21_13_14_32_20ms 2016_10_21_13_16_20_30ms 2016_10_21_13_17_21_40ms 2016_10_21_13_18_22_50ms 2016_10_21_13_15_21_25ms 2016_10_21_13_16_47_35ms 2016_10_21_13_17_51_45ms 2016_10_21_13_18_52_55ms ; do
    /bin/rm -f ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir*px1*
    xps_tm=$(echo ${drc} | cut -d '_' -f 7)
    ncks -O --trr var_nm=xps_img_wht --trr_wxy=955,1,1 --trr typ_in=NC_USHORT --trr typ_out=NC_USHORT --trr ntl_in=bil --trr ntl_out=bsq --trr ttl="Spectralon target with nominal visible reflectance = 0.95, as exposed to VNIR single pixel, single scanline on 20161021 ~13:15 local time in ${drc}" --trr_in=${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/CorrectedWhite_raw ~/terraref/computing-pipeline/scripts/hyperspectral/hyperspectral_dummy.nc ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_px1_${xps_tm}.nc
    /bin/cp ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_px1_${xps_tm}.nc ${drc_clb}
done    

# White reference, image:
for drc in 2016_10_21_13_14_32_20ms 2016_10_21_13_16_20_30ms 2016_10_21_13_17_21_40ms 2016_10_21_13_18_22_50ms 2016_10_21_13_15_21_25ms 2016_10_21_13_16_47_35ms 2016_10_21_13_17_51_45ms 2016_10_21_13_18_52_55ms ; do
    /bin/rm -f ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir*img* ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir*cut* ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir*avg*
    xps_tm=$(echo ${drc} | cut -d '_' -f 7)
    hdr_fl=${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/raw.hdr
    ydm_nbr=$(grep '^lines' ${hdr_fl} | cut -d ' ' -f 3 | tr -d '\015')
    echo "Calibration file ${drc}/raw has ${ydm_nbr} lines"
    ncks -O --trr var_nm=xps_img_wht --trr_wxy=955,1600,${ydm_nbr} --trr typ_in=NC_USHORT --trr typ_out=NC_USHORT --trr ntl_in=bil --trr ntl_out=bsq --trr ttl="Spectralon target with nominal visible reflectance = 0.95, as exposed to VNIR full image 1600 pixel and 268-298 lines on 20161021 ~13:15 local time in ${drc}. Spectralon is located in lines ~35-90 and samples (pixels) 600-1000." --trr_in=${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/raw ~/terraref/computing-pipeline/scripts/hyperspectral/hyperspectral_dummy.nc ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_img_${xps_tm}.nc
# Visual inspection shows following hyperslab matches Spectralon location and shape
    ncks -O -F -d x,600,1000 -d y,35,90 ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_img_${xps_tm}.nc ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_cut_${xps_tm}.nc
    ncwa -O -a x,y ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_cut_${xps_tm}.nc ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_avg_${xps_tm}.nc
    /bin/cp ${HOME}/Downloads/VNIR_SpectralonRef_SinglePixel/${drc}/vnir_wht_avg_${xps_tm}.nc ${drc_clb}
done

# Dark reference:
# 7z l VNIR-DarkRef.7z # List files
# 7z e VNIR-DarkRef.7z # Extract all files to ${CWD}
# 7z x VNIR-DarkRef.7z # Extract files with full paths
for drc in 2016_10_19_02_58_32-20ms 2016_10_19_03_00_27-30ms 2016_10_19_03_01_51-40ms 2016_10_19_04_16_44-50ms 2016_10_19_02_59_35-25ms 2016_10_19_03_01_07-35ms 2016_10_19_04_16_16-45ms 2016_10_19_04_17_07-55ms ; do
    /bin/rm -f ${HOME}/Downloads/VNIR-DarkRef/${drc}/vnir*img* ${HOME}/Downloads/VNIR-DarkRef/${drc}/vnir*avg*
    xps_tm=$(echo ${drc} | cut -d '_' -f 6)
    xps_tm=$(echo ${xps_tm} | cut -d '-' -f 2)
    hdr_fl=${HOME}/Downloads/VNIR-DarkRef/${drc}/raw.hdr
    ydm_nbr=$(grep '^lines' ${hdr_fl} | cut -d ' ' -f 3 | tr -d '\015')
    echo "Calibration file ${drc}/raw has ${ydm_nbr} lines"
    ncks -O --trr var_nm=xps_img_drk --trr_wxy=955,1600,${ydm_nbr} --trr typ_in=NC_USHORT --trr typ_out=NC_USHORT --trr ntl_in=bil --trr ntl_out=bsq --trr ttl="Dark counts as exposed to VNIR full image 1600 pixel and 182-218 lines on 20161019 ~3-4 AM local time in ${drc}." --trr_in=${HOME}/Downloads/VNIR-DarkRef/${drc}/raw ~/terraref/computing-pipeline/scripts/hyperspectral/hyperspectral_dummy.nc ${HOME}/Downloads/VNIR-DarkRef/${drc}/vnir_drk_img_${xps_tm}.nc
#   20161031 Dark image data fits well. No hyperslabbing necessary. Some wavelength-dependent (though unpredictable) structure in X. Little-to-no Y structure.
    ncwa -O -a x,y ${HOME}/Downloads/VNIR-DarkRef/${drc}/vnir_drk_img_${xps_tm}.nc ${HOME}/Downloads/VNIR-DarkRef/${drc}/vnir_drk_avg_${xps_tm}.nc
    /bin/cp ${HOME}/Downloads/VNIR-DarkRef/${drc}/vnir_drk_avg_${xps_tm}.nc ${drc_clb}
done

# Combine dark and white reference exposures in single file for each exposure duration
for xps_tm in 20ms 25ms 30ms 35ms 40ms 45ms 50ms 55ms ; do
    ncks -O -v xps_img_wht ${drc_clb}/vnir_wht_avg_${xps_tm}.nc ${drc_clb}/calibration_vnir_${xps_tm}.nc
    ncks -A -v xps_img_drk ${drc_clb}/vnir_drk_avg_${xps_tm}.nc ${drc_clb}/calibration_vnir_${xps_tm}.nc
done

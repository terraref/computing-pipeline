#!/bin/bash

dataset() {
  FOLDER="$1"
  DSPATH="$2"
  TITLE="$3"

  echo "    <dataset name=\"${TITLE}\" ID=\"${TITLE}\">"

  IFS=$'\n'
  LAST_DATE=""
  LAST_TIME=""

  FILES=$( find ${FOLDER} -name \*.nc | sort )
  #FILES=$( cat ncfiles.txt | sort )
  for X in ${FILES}; do
      # remove leading whitespace, and extract information
      X="${X#${FOLDER}}"
      X="${X//[[:space:]]/}"
      X="${X:1}"
      echo "$X"

      DATE=$( echo "$X" | cut -d "/" -f1 )
      if [ "$DATE" != "$LAST_DATE" ]; then
        if [ "$LAST_DATE" != "" ]; then
          echo '        </dataset>'
          echo '      </dataset>'
        fi
        LAST_TIME=""
        LAST_DATE="$DATE"
        echo "      <dataset name=\"${DATE}\" ID=\"${DATE}\">"
      fi

      TIME=$( echo "$X" | cut -d "/" -f2)
      if [ "$TIME" != "$LAST_TIME" ]; then
        if [ "$LAST_TIME" != "" ]; then
          echo '        </dataset>'
        fi
        LAST_TIME="$TIME"
        NAME=$( echo "$TIME" | cut -d "_" -f3 | tr "-" ":" | cut -d ":" -f 1,2 )
        echo "        <dataset name=\"${NAME}\" ID=\"${TIME}\">"
      fi

      NAME=$( echo "$X" | cut -d "/" -f3 )
      echo "          <dataset name=\"${NAME}\" ID=\"${X}\" urlPath=\"${DSPATH}${X}\" serviceName=\"all\">"
      echo '          </dataset>'
  done
  echo '        </dataset>'
  echo '      </dataset>'
  echo '    </dataset>'
}

# ----------------------------------------------------------------------
# HEADER
# ----------------------------------------------------------------------
cat << EOF
<?xml version="1.0" encoding="UTF-8"?>
<catalog name="THREDDS Server Default Catalog : You must change this to fit your server!"
         xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"
         xmlns:xlink="http://www.w3.org/1999/xlink"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0
           http://www.unidata.ucar.edu/schemas/thredds/InvCatalog.1.0.6.xsd">

  <service name="all" base="" serviceType="compound">
    <service name="odap" serviceType="OpenDAP" base="/thredds/dodsC/" />
    <service name="dap4" serviceType="DAP4" base="/thredds/dap4/" />
    <service name="http" serviceType="HTTPServer" base="/thredds/fileServer/" />
    <!--service name="wcs" serviceType="WCS" base="/thredds/wcs/" /-->
    <!--service name="wms" serviceType="WMS" base="/thredds/wms/" /-->
    <service name="ncss" serviceType="NetcdfSubset" base="/thredds/ncss/" />
  </service>

  <datasetRoot path="uamac_hs" location="/home/sites/ua-mac/Level_1/vnir_netcdf"/>

  <dataset name="TERRA" ID="TERRA">
EOF

# ----------------------------------------------------------------------
# DATASETS
# ----------------------------------------------------------------------
dataset "/home/sites/ua-mac/Level_1/vnir_netcdf" "uamac_hs" "UAMac Hyperspectral"

# ----------------------------------------------------------------------
# FOOTER
# ----------------------------------------------------------------------
echo '  </dataset>'
echo '</catalog>'

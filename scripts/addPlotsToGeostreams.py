import sys, os, json
import requests
from osgeo import gdal
from osgeo import ogr
from osgeo import osr


"""
This script will push polygons in a shapefile into Geostreams database as sensors (plots).

Plot name will be checked to avoid duplication.

SENSORS
 gid |      name       |                                geog                                |            created            |                                                                          metadata
-----+-----------------+--------------------------------------------------------------------+-------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------
  13 | Range 41 Pass 2 | 01010000A0E6100000954E254166FE5BC0EE08D3E2B98940400000000000000000 | 2017-02-02 11:45:26.501508-06 | {"type":{"id":"LemnaTec","title":"LemnaTec Field Scanalyzer","sensorType":4},"name":"Range 41 Pass 2","popupContent":"Range 41 Pass 2","region":"Maricopa"}
  14 | Range 14 Pass 4 | 01010000A0E6100000F030722B65FE5BC041CE78169A8940400000000000000000 | 2017-02-02 11:45:27.499092-06 | {"type":{"id":"LemnaTec","title":"LemnaTec Field Scanalyzer","sensorType":4},"name":"Range 14 Pass 4","popupContent":"Range 14 Pass 4","region":"Maricopa"}
  15 | Range 20 Pass 6 | 01010000A0E610000090D7C49263FE5BC0358B04D8C68940400000000000000000 | 2017-02-02 11:45:28.511511-06 | {"type":{"id":"LemnaTec","title":"LemnaTec Field Scanalyzer","sensorType":4},"name":"Range 20 Pass 6","popupContent":"Range 20 Pass 6","region":"Maricopa"}
"""


# Get sensor ID from Clowder based on plot name
def get_sensor_id(host, key, name):
    if(not host.endswith("/")):
        host = host+"/"

    url = "%sapi/geostreams/sensors?sensor_name=%s&key=%s" % (host, name, key)
    logging.debug("...searching for sensor : "+name)
    r = requests.get(url)
    if r.status_code == 200:
        json_data = r.json()
        for s in json_data:
            if 'name' in s and s['name'] == name:
                return s['id']
    else:
        print("error searching for sensor ID")

    return None

def create_sensor(host, key, name, geom):
    if(not host.endswith("/")):
        host = host+"/"

    body = {
        "name": name,
        "type": "point",
        "geometry": geom,
        "properties": {
            "popupContent": name,
            "type": {
                "id": "MAC Field Scanner",
                "title": "MAC Field Scanner",
                "sensorType": 4
            },
            "name": name,
            "region": "Maricopa"
        }
    }

    url = "%sapi/geostreams/sensors?key=%s" % (host, key)
    logging.info("...creating new sensor: "+name)
    r = requests.post(url,
                      data=json.dumps(body),
                      headers={'Content-type': 'application/json'})
    if r.status_code == 200:
        return r.json()['id']
    else:
        logging.error("error creating sensor")

    return None


shpFile = sys.argv[1]

# OPEN SHAPEFILE
ds = gdal.OpenEx( shpFile, gdal.OF_VECTOR | gdal.OF_READONLY)
layerName = os.path.basename(shpFile).split('.shp')[0]
lyr = ds.GetLayerByName(layerName)

# load shp file
lyr.ResetReading()
num_records = lyr.GetFeatureCount()
lyr_defn = lyr.GetLayerDefn()
t_srs = lyr.GetSpatialRef()
s_srs = osr.SpatialReference()
s_srs.ImportFromEPSG(4326)
transform = osr.CoordinateTransformation(s_srs, t_srs)
transform_back = osr.CoordinateTransformation(t_srs, s_srs)

fi_rangepass = lyr_defn.GetFieldIndex('RangePass')
fi_range = lyr_defn.GetFieldIndex('Range')
fi_pass = lyr_defn.GetFieldIndex('Pass')
fi_macentry = lyr_defn.GetFieldIndex('MAC_ENTRY')

# ITERATE OVER FEATURES
for f in lyr:
    # DETERMINE SENSOR NAME
    plotid = f.GetFieldAsString(fi_rangepass)
    geom = f.GetGeometryRef()

    geom.Transform(transform_back)
    centroid = geom.Centroid()

    # POST IT IF IT DOESN'T EXIST
    host = "http://terraref.ncsa.illinois.edu/clowder"
    key = ""

    plot_name = "Range "+plotid.replace("-", " Pass ")
    sensor_id = get_sensor_id(host, key, plot_name)
    if not sensor_id:
        sensor_id = create_sensor(host, key, plot_name, {
            "type": "Point",
            "coordinates": [centroid.GetX(), centroid.GetY(), 0]
        })

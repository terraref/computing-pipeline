#!/usr/bin/python

""" IP to GEOHASH CONVERTER

    Given an IP address as an argument, this script will query GeoIP2 free database to estimate
    the IP location latitutde/longitude and from there return a geohash.

    Usage:
        python ip2geohash.py 192.168.99.100
"""

import Geohash
import geoip2.database
import sys


ip_address = sys.argv[1]

try:
    reader = geoip2.database.Reader("GeoLite2-City.mmdb")
except:
    print("GeoLite2-City.mmdb must be in same directory as script.")
    exit(1)

response = reader.city(ip_address)

latlon = (response.location.latitude,
          response.location.longitude)

print(Geohash.encode(response.location.latitude,
                     response.location.longitude))

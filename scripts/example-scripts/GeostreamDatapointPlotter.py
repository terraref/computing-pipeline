import requests
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

url = "https://terraref.ncsa.illinois.edu/clowder/api/geostreams/datapoints?key=Pb3AUSqnUw&stream_id=300"

response = requests.get(url)
if response.status_code == 200:
    json = response.json()
    
    times = []
    wind_speeds = []
    precipitation_rate = []
    surface_downwelling_shortwave_flux_in_air = []
    northward_wind = []
    relative_humidity = []
    air_temperature = []
    eastward_wind = []
    surface_downwelling_photosynthetic_photon_flux_in_air = []
    
    for datapoint in json:
        """Example datapoint:
        {   'sensor_id': '303',
            'created': '2017-02-03T14:33:11Z',
            'geometry': {
                'type': 'Point',
                'coordinates': [33.0745666667, -68.0249166667, 0]
            },
            'start_time': '2016-08-10T13:50:29Z',
            'id': 36185,
            'stream_id': '300',
            'sensor_name': 'Full Field',
            'end_time': '2016-08-10T13:55:00Z',
            'type': 'Feature',
            'properties': {
                'wind_speed': 1.0890774907749077,
                'precipitation_rate': 0.0,
                'surface_downwelling_shortwave_flux_in_air': 43.60608856088568,
                'northward_wind': -0.9997966833626167,
                'relative_humidity': 60.41579335793356,
                'source': u'https://terraref.ncsa.illinois.edu/clowder/datasets/5893a72c4f0c06726b1b0cda',
                'source_file': u'5893a72f4f0c06726b1b0d20',
                'air_temperature': 301.13597785977885,
                'eastward_wind': -0.3659132309673836,
                'surface_downwelling_photosynthetic_photon_flux_in_air': 152.14981549815525
        }}"""
        
        start_time = datapoint['start_time']
        times.append(datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ"))
        
        p = datapoint['properties']
        wind_speeds.append(p['wind_speed'])
        precipitation_rate.append(p['precipitation_rate'])
        surface_downwelling_shortwave_flux_in_air.append(p['surface_downwelling_shortwave_flux_in_air'])
        northward_wind.append(p['northward_wind'])
        relative_humidity.append(p['relative_humidity'])
        air_temperature.append(p['air_temperature'])
        eastward_wind.append(p['eastward_wind'])
        surface_downwelling_photosynthetic_photon_flux_in_air.append(p['surface_downwelling_photosynthetic_photon_flux_in_air'])

    plt.figure(1)
    plt.title("Wind Speed")
    plt.plot(times, wind_speeds, 'b-')
    
    plt.figure(2)
    plt.title("Precipitation Rate")
    plt.plot(times, precipitation_rate, 'r-')
    
    plt.figure(3)
    plt.title("Surface Downwelling Shortwave Flux in Air")
    plt.plot(times, surface_downwelling_shortwave_flux_in_air, 'g-')

    plt.figure(4)
    plt.title("Northward Wind")
    plt.plot(times, northward_wind, 'c-')

    plt.figure(5)
    plt.title("Relative Humidity")
    plt.plot(times, relative_humidity, 'm-')

    plt.figure(6)
    plt.title("Air Temperature, K")
    plt.plot(times, air_temperature, 'y-')

    plt.figure(7)
    plt.title("Eastward Wind")
    plt.plot(times, eastward_wind, 'k-')

    plt.figure(8)
    plt.title("Surface Downwelling Photosynthetic Photon Flux in Air")
    plt.plot(times, surface_downwelling_photosynthetic_photon_flux_in_air, 'b-')
    
    plt.show()
else:
    print("no response")

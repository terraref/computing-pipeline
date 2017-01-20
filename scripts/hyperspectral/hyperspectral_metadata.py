#!/usr/bin/env python

'''
Created on Feb 5, 2016

Purpose: Parse JSON-formatted metadata and data and header provided by LemnaTec to produce netCDF4 output

@author: jeromemao
----------------------------------------------------------------------------------------
This script works with both Python 2.7+ and 3+, depending on the netCDF4 module version
Thanks for the advice from Professor Zender and sample data from Dr. LeBauer.
----------------------------------------------------------------------------------------
Usage (commandline):
python hyperspectral_metadata.py dbg=yes fmt=4 ftn=no filePath1 filePath2

where
hyperspectral_metadata.py is where this script located
filePath1 is source data file
filePath2 is user's desired output file
fmt (format) is the format of the output file; it can be netCDF4 or netCDF3 ("3" or "4")
ftn (flatten) is whether flatten the output file; if yes, all the variables and attributes will be in root groups ("yes" or "no")

Please note that since netCDF3 does NOT support individual groups, the execution with fmt=3 will be flatten no matter the option for ftn

Warning:
Make sure the json metadata ended with <data_name>+_metadata.json and the hdr file ended with <data_name>+_raw.hdr
For example, if you have a group of data named like this:

Data: data_raw
Metadata: data_metadata.json
Header: data_raw.hdr

Usage:
python hyperspectral_metadata.py dbg=yes fmt=4 ftn=no in.json out.nc

Example:
python ${HOME}/terraref/computing-pipeline/scripts/hyperspectral/hyperspectral_metadata.py dbg=yes fmt=4 ftn=no ${DATA}/terraref/VNIR/2016-10-06/2016-10-06__15-21-20-178/b73a4f00-4140-4576-8c70-8e1d26ae245e_raw ~/foo.nc

If pointed to a directory rather than a file, hyperspectral_metadata.py will authomatically find data_raw, data_metadata.json and data_raw.hdr
----------------------------------------------------------------------------------------
UPDATE LOG (reverse chronological order):

Update 20160901:
Rename from JsonDealer.py to hyperspectral_metadata.py

Update 20160822:
Fix major bugs, including:
1. file checking functions now works as expectedly by reimplemented with regular expression
2. the data from "user_given_metadata" are saved as group attributes except those time variables
3. translate_time() now could calculate either time since Unix base time or the time split between certain time points
Other improvements including better implementations on DataContainer and a more friendly prompts to users
when the output file had already existed.

Update 20160509:
Now the JsonDealer.py will also parse the data from frameIndex.txt
the time-related variable (except history) will be recorded as the offset to the _UNIX_BASETIME

Update 20160425:
Attributes and variables now looks nicer.
Rename "Velocity ..." as "Gantry Speed ..."
Set the "default bands" variable from the header file as attributes of "exposure" variable.

Update 20160412:
Fixed bugs in getting dimensions from the header file.

Update 20160401:
Merged with DataProcess module; now JsonDealer will do all the jobs.

hyperspectral_metadata widely uses regular expressions to match string; although most of them are compatible
with Java, PHP, Perl, etc., some of the regular expressions are only supported by the Python standard.
----------------------------------------------------------------------------------------
'''
import sys
try:
    import numpy as np
except ImportError as AFatalError:
    print >> sys.stderr, AFatalError
    exit()
import json
import time
import os
import re
import struct
from datetime import date, datetime
from netCDF4 import Dataset, stringtochar
from hyperspectral_calculation import pixel2Geographic, REFERENCE_POINT

_UNIT_DICTIONARY = {'m':   'meter',
                    's':   'second', 
                    'm/s': 'meter second-1', 
                    '':    ''}

_VELOCITY_DICTIONARY = {'x': 'u', 
                        'y': 'v', 
                        'z': 'w'}

DATATYPE = {'1' : ('H', 2), 
            '2' : ('i', 4), 
            '3' : ('l', 4), 
            '4' : ('f', 4), 
            '5' : ('d', 8), 
            '12': ('H', 4), 
            '13': ('L', 4), 
            '14': ('q', 8), 
            '15': ('Q', 8)}

_IS_DIGIT         = lambda fakeNum: set([member.isdigit() for member in fakeNum.split(".")]) == {True}
_TIMESTAMP        = lambda: time.strftime("%a %b %d %H:%M:%S %Y",  time.localtime(int(time.time())))

_WARN_MSG         = "{msg}"


class DataContainer(object):
    '''
    A class which saves the data from Json file
    '''

    def __init__(self, source):
        for members in source[u'lemnatec_measurement_metadata']:
            setattr(self, members, source[u'lemnatec_measurement_metadata'][members])

    def __str__(self):
        result = str()
        for members in self.__dict__:
            result += (str(members) + ': ' +
                       str(self.__dict__[members]) + '\n')
        return result

    def __getitem__(self, param):
        if param in self.__dict__:
            return self.__dict__[param]

    def writeToNetCDF(self, inputFilePath, outputFilePath, commandLine, format, flatten=False, _debug=True):
        # weird, but useful to check whether the HeaderInfo id in the netCDF
        # file
        setattr(self, "header_info", None)
        netCDFHandler = _file_existence_check(outputFilePath, format, self)
        delattr(self, "header_info")

        #### default camera is SWIR, but will see based on the number of wavelengths
        camera_opt = "SWIR"

        ##### Write the data from metadata to netCDF #####
        for key, data in self.__dict__.items():
            tempGroup = netCDFHandler.createGroup(key) if not flatten else netCDFHandler
            for subkey, subdata in data.items():
                if not _IS_DIGIT(subdata): #Case for letter variables

                    ##### For date variables #####
                    if 'date' in subkey and subkey != "date of installation" and subkey != "date of handover":
                        assert subdata != "todo", '"todo" is not a legal value for the keys'

                        tempVariable = tempGroup.createVariable(_reformat_string(subkey), 'f8')
                        tempVariable[...] = translate_time(subdata)
                        setattr(tempVariable, "units",     "days since 1970-01-01 00:00:00")
                        setattr(tempVariable, "calender", "gregorian")

                    setattr(tempGroup, _reformat_string(subkey), subdata)

                else: #Case for digits variables
                    if "time" in data:
                        yearMonthDate = data["time"]
                    elif "Time" in data:
                        yearMonthDate = data["Time"]
                    setattr(tempGroup, _reformat_string(subkey), subdata)

                    short_name, attributes = _generate_attr(subkey)
                    tempVariable = tempGroup.createVariable(short_name, 'f8')
                    for name, value in attributes.items():
                        setattr(tempVariable, name, value)
                    tempVariable[...] = float(subdata)

        ##### Write data from header files to netCDF #####
        wavelength = get_wavelength(inputFilePath)
        netCDFHandler.createDimension("wavelength", len(wavelength))

        # Check if the wavelength is correctly collected
        assert len(wavelength) in (955, 272), "ERROR: Failed to get wavlength informations. Please check if you modified the *.hdr files"

        camera_opt = 'VNIR' if len(wavelength) == 955 else 'SWIR' # Choose appropriate camera by counting the number of wavelengths.

        tempWavelength = netCDFHandler.createVariable("wavelength", 'f8', 'wavelength')
        setattr(tempWavelength, 'long_name', 'Hyperspectral Wavelength')
        setattr(tempWavelength, 'units', 'nanometers')
        tempWavelength[...] = wavelength
        write_header_file(inputFilePath, netCDFHandler, flatten, _debug)

        ##### Write the data from frameIndex files to netCDF #####
        tempFrameTime = frame_index_parser(''.join((inputFilePath.strip("raw"), "frameIndex.txt")), yearMonthDate)
        netCDFHandler.createDimension("time", len(tempFrameTime))

        # Check if the frame time information is correctly collected
        assert len(tempFrameTime), "ERROR: Failed to collect frame time information from " + ''.join((inputFilePath.strip("raw"), "frameIndex.txt")) + ". Please check the file."
       
        frameTime      = netCDFHandler.createVariable("frametime", "f8", ("time",))
        frameTime[...] = tempFrameTime
        setattr(frameTime, "units",    "days since 1970-01-01 00:00:00")
        setattr(frameTime, "calender", "gregorian")
        setattr(frameTime, "notes",    "Each time of the scanline of the y taken")

        ########################### Adding geographic positions ###########################

        xPixelsLocation, yPixelsLocation, boundingBox, googleMapAddress\
         = pixel2Geographic("".join((inputFilePath[:-4],"_metadata.json")), "".join((inputFilePath,'.hdr')), camera_opt)

        # Check if the image width and height are correctly collected.
        assert len(xPixelsLocation) > 0 and len(yPixelsLocation) > 0, "ERROR: Failed to collect the image size metadata from " + "".join((inputFilePath,'.hdr')) + ". Please check the file."
        
        netCDFHandler.createDimension("x", len(xPixelsLocation))
        x    = netCDFHandler.createVariable("x", "f8", ("x",))
        x[...] = xPixelsLocation
        setattr(netCDFHandler.variables["x"], "units", "meters")
        setattr(netCDFHandler.variables['x'], 'reference_point', 'Southeast corner of field')
        setattr(netCDFHandler.variables['x'], "long_name", "North-south offset from southeast corner of field")

        netCDFHandler.createDimension("y", len(yPixelsLocation))
        y    = netCDFHandler.createVariable("y", "f8", ("y",))
        y[...] = yPixelsLocation
        setattr(netCDFHandler.variables["y"], "units", "meters")
        setattr(netCDFHandler.variables['y'], 'reference_point', 'Southeast corner of field')
        setattr(netCDFHandler.variables['y'], "long_name", "Distance west of the southeast corner of the field")

        lat_pt, lon_pt = REFERENCE_POINT

        lat_pt_var = netCDFHandler.createVariable("lat_reference_point", "f8")
        lat_pt_var[...] = lat_pt
        setattr(netCDFHandler.variables["lat_reference_point"], "units", "degrees_north")
        setattr(netCDFHandler.variables["lat_reference_point"], "long_name", "Latitude of the master reference point at southeast corner of field")
        setattr(netCDFHandler.variables["lat_reference_point"], "provenance", "https://github.com/terraref/reference-data/issues/32 by Dr. David LeBauer")

        lon_pt_var = netCDFHandler.createVariable("lon_reference_point", "f8")
        lon_pt_var[...] = lon_pt
        setattr(netCDFHandler.variables["lon_reference_point"], "units", "degrees_east")
        setattr(netCDFHandler.variables["lon_reference_point"], "long_name", "Longitude of the master reference point at southeast corner of field")
        setattr(netCDFHandler.variables["lon_reference_point"], "provenance", "https://github.com/terraref/reference-data/issues/32 by Dr. David LeBauer")

        x_ref_pt = netCDFHandler.createVariable("x_reference_point", "f8")
        x_ref_pt[...] = 0
        setattr(netCDFHandler.variables["x_reference_point"], "units", "meters")
        setattr(netCDFHandler.variables["x_reference_point"], "long_name", "x of the master reference point at southeast corner of field")
        setattr(netCDFHandler.variables["x_reference_point"], "provenance", "https://github.com/terraref/reference-data/issues/32 by Dr. David LeBauer")

        y_ref_pt = netCDFHandler.createVariable("y_reference_point", "f8")
        y_ref_pt[...] = 0
        setattr(netCDFHandler.variables["y_reference_point"], "units", "meters")
        setattr(netCDFHandler.variables["y_reference_point"], "long_name", "y of the master reference point at southeast corner of field")
        setattr(netCDFHandler.variables["y_reference_point"], "provenance", "https://github.com/terraref/reference-data/issues/32 by Dr. David LeBauer")

        # Write latitude and longitude of bounding box
        SE, SW, NE, NW = boundingBox[0], boundingBox[1], boundingBox[2], boundingBox[3]
        lat_se, lon_se = tuple(SE.split(", "))
        lat_sw, lon_sw = tuple(SW.split(", "))
        lat_ne, lon_ne = tuple(NE.split(", "))
        lat_nw, lon_nw = tuple(NW.split(", "))

        latSe = netCDFHandler.createVariable("lat_img_se", "f8")
        latSe[...] = float(lat_se)
        setattr(netCDFHandler.variables["lat_img_se"], "units", "degrees_north")
        setattr(netCDFHandler.variables["lat_img_se"], "long_name", "Latitude of southeast corner of image")

        # have a "x_y_img_se" in meters, double
        lonSe = netCDFHandler.createVariable("lon_img_se", "f8")
        lonSe[...] = float(lon_se)
        setattr(netCDFHandler.variables["lon_img_se"], "units", "degrees_east")
        setattr(netCDFHandler.variables["lon_img_se"], "long_name", "Longitude of southeast corner of image")

        latSw = netCDFHandler.createVariable("lat_img_sw", "f8")
        latSw[...] = float(lat_sw)
        setattr(netCDFHandler.variables["lat_img_sw"], "units", "degrees_north")
        setattr(netCDFHandler.variables["lat_img_sw"], "long_name", "Latitude of southwest corner of image")

        lonSw = netCDFHandler.createVariable("lon_img_sw", "f8")
        lonSw[...] = float(lon_sw)
        setattr(netCDFHandler.variables["lon_img_sw"], "units", "degrees_east")
        setattr(netCDFHandler.variables["lon_img_sw"], "long_name", "Longitude of southwest corner of image")

        latNe = netCDFHandler.createVariable("lat_img_ne", "f8")
        latNe[...] = float(lat_ne)
        setattr(netCDFHandler.variables["lat_img_ne"], "units", "degrees_north")
        setattr(netCDFHandler.variables["lat_img_ne"], "long_name", "Latitude of northeast corner of image")

        lonNe = netCDFHandler.createVariable("lon_img_ne", "f8")
        lonNe[...] = float(lon_ne)
        setattr(netCDFHandler.variables["lon_img_ne"], "units", "degrees_east")
        setattr(netCDFHandler.variables["lon_img_ne"], "long_name", "Longitude of northeast corner of image")

        latNw = netCDFHandler.createVariable("lat_img_nw", "f8")
        latNw[...] = float(lat_nw)
        setattr(netCDFHandler.variables["lat_img_nw"], "units", "degrees_north")
        setattr(netCDFHandler.variables["lat_img_nw"], "long_name", "Latitude of northwest corner of image")

        lonNw = netCDFHandler.createVariable("lon_img_nw", "f8")
        lonNw[...] = float(lon_nw)
        setattr(netCDFHandler.variables["lon_img_nw"], "units", "degrees_east")
        setattr(netCDFHandler.variables["lon_img_nw"], "long_name", "Longitude of northwest corner of image")

        xSe = netCDFHandler.createVariable("x_img_se", "f8")
        xSe[...] = float(x[-1])
        setattr(netCDFHandler.variables["x_img_se"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_se"], "long_name", "Southeast corner of image, north distance to reference point")

        # have a "x_y_img_se" in meters, double
        ySe = netCDFHandler.createVariable("y_img_se", "f8")
        ySe[...] = float(y[-1])
        setattr(netCDFHandler.variables["y_img_se"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_se"], "long_name", "Southeast corner of image, west distance to reference point")

        xSw = netCDFHandler.createVariable("x_img_sw", "f8")
        xSw[...] = float(x[0])
        setattr(netCDFHandler.variables["x_img_sw"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_sw"], "long_name", "Southwest corner of image, north distance to reference point")

        ySw = netCDFHandler.createVariable("y_img_sw", "f8")
        ySw[...] = float(y[-1])
        setattr(netCDFHandler.variables["y_img_sw"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_sw"], "long_name", "Southwest corner of image, west distance to reference point")

        xNe = netCDFHandler.createVariable("x_img_ne", "f8")
        xNe[...] = float(x[-1])
        setattr(netCDFHandler.variables["x_img_ne"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_ne"], "long_name", "Northeast corner of image, north distance to reference point")

        yNe = netCDFHandler.createVariable("y_img_ne", "f8")
        yNe[...] = float(y[0])
        setattr(netCDFHandler.variables["y_img_ne"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_ne"], "long_name", "Northeast corner of image, west distance to reference point")

        xNw = netCDFHandler.createVariable("x_img_nw", "f8")
        xNw[...] = float(x[0])
        setattr(netCDFHandler.variables["x_img_nw"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_nw"], "long_name", "Northwest corner of image, north distance to reference point")

        yNw = netCDFHandler.createVariable("y_img_nw", "f8")
        yNw[...] = float(y[0])
        setattr(netCDFHandler.variables["y_img_nw"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_nw"], "long_name", "Northwest corner of image, west distance to reference point")
        
        if format == "NETCDF3_CLASSIC":
            netCDFHandler.createDimension("length of Google Map String", len(googleMapAddress))
            googleMapView = netCDFHandler.createVariable("Google_Map_View", "S1", ("length of Google Map String",))
            tempAddress = np.chararray((1, 1), itemsize=len(googleMapAddress))
            tempAddress[:] = googleMapAddress
            googleMapView[...] = stringtochar(tempAddress)[0]
        else:
            googleMapView = netCDFHandler.createVariable("Google_Map_View", str)
            googleMapView[...] = googleMapAddress

        setattr(netCDFHandler.variables["Google_Map_View"], "usage", "copy and paste to your web browser")
        setattr(netCDFHandler.variables["Google_Map_View"], 'reference_point', 'Southeast corner of field')

        y_pxl_sz = netCDFHandler.createVariable("y_pxl_sz", "f8")
        y_pxl_sz[...] = 0.98526434004512529576754637665e-3
        setattr(netCDFHandler.variables["y_pxl_sz"], "units", "meters")
        setattr(netCDFHandler.variables["y_pxl_sz"], "notes", "y coordinate length of a single pixel in pictures captured by SWIR and VNIR camera")

        if camera_opt == "SWIR":
            x_pxl_sz = netCDFHandler.createVariable("x_pxl_sz", "f8")
            x_pxl_sz[...] = 1.025e-3
            setattr(netCDFHandler.variables["x_pxl_sz"], "units", "meters")
            setattr(netCDFHandler.variables["x_pxl_sz"], "notes", "x coordinate length of a single pixel in SWIR images")

        else:
            x_pxl_sz = netCDFHandler.createVariable("x_pxl_sz", "f8")
            x_pxl_sz[...] = 1.930615052e-3
            setattr(netCDFHandler.variables["x_pxl_sz"], "units", "meters")
            setattr(netCDFHandler.variables["x_pxl_sz"], "notes", "x coordinate length of a single pixel in VNIR images")

        ##### Write the history to netCDF #####
        netCDFHandler.history = ''.join((_TIMESTAMP(), ': python ', commandLine))

        netCDFHandler.close()

def getDimension(fileName, _debug=True):
    '''
    Acquire dimensions from related HDR file; these dimensions are:
    samples -> 'x'
    lines   -> 'y'
    bands   -> 'wavelength'
    '''
    with open("".join((fileName, '.hdr'))) as fileHandler:
        for members in fileHandler.read().splitlines():
            if "samples" == members[:7]:
                x = members[members.find("=") + 1:len(members)]
            elif "lines" == members[:5]:
                y = members[members.find("=") + 1:len(members)]
            elif "bands" == members[:5]:
                wavelength = members[members.find("=") + 1:len(members)]

        try:
            return int(wavelength),\
                   int(x),\
                   int(y)

        except:
            if _debug:
                print >> sys.stderr, _WARN_MSG.format(msg='ERROR: sample, lines and bands variables in header file are broken. Header information will not be written into the netCDF')
            return 0, 0, 0

def get_wavelength(fileName):
    '''
    Acquire wavelength(s) from related HDR file
    '''
    with open("".join((fileName, '.hdr'))) as fileHandler:
        wavelengthGroup = [float(x.strip(',')) for x in fileHandler.read().splitlines() if _IS_DIGIT(x.strip(','))]
        return wavelengthGroup


def get_header_info(fileName):
    '''
    Acquire Other Information from related HDR file
    '''
    with open("".join((fileName, '.hdr'))) as fileHandler:
        infoDictionary = {members[0:members.find("=") - 1].strip(";") : members[members.find("=") + 2:] for members in fileHandler.read().splitlines() if '=' in members and 'wavelength' not in members}
        return infoDictionary


def _file_existence_check(filePath, fmt, dataContainer):
    '''
    This method will check wheter the filePath has the same variable name as the dataContainer has. If so,
    user will decide whether skip or overwrite it (no append,
    since netCDF does not support the repeating variable names)
    '''
    userPrompt = _WARN_MSG.format(msg='--> Output file already exists; skip it or overwrite or append? (\033[4;31mS\033[0;31mkip, \033[4;31mO\033[0;31mverwrite, \033[4;31mA\033[0;31mppend)\033[0m')

    if os.path.isdir(filePath):
        filePath += "".join(("/", filePath.split("/")[-1], ".nc"))

    if os.path.exists(filePath):
        netCDFHandler = Dataset(filePath, 'r', format=fmt)
        if set([x.encode('utf-8') for x in netCDFHandler.groups]) - \
           set([x for x in dataContainer.__dict__]) != set([x.encode('utf-8') for x in netCDFHandler.groups]):

            while True:
                userChoice = str(raw_input(userPrompt))

                if userChoice is 'S':
                    print >> sys.stderr, "Exit due to the skipping"
                    exit()
                elif userChoice in ('O', 'A'):
                    os.remove(filePath)
                    return Dataset(filePath, 'w', format=fmt)
        else:
            os.remove(filePath)
    return Dataset(filePath, 'w', format=fmt)

def _reformat_string(string):
    '''
    This method will replace spaces (' '), slashes('/') and many other unwanted characters
    '''

    string = string.replace('/', '_per_').replace(' ', '_')

    if '(' in string:
        return string[:string.find('(') - 1]
    elif '[' in string:
        return string[:string.find('[') - 1]

    return string

def _generate_attr(string):
    '''
    This method will parse the string to a group of long names, short names and values
    Position and Velocity variables will be specially treated
    '''

    long_name = ""
    if "current setting" in string:
        long_name = string.split(' ')[-1]
    elif "speed" in string and "current setting" not in string:
        long_name = "".join(('Gantry Speed in ', string[6].upper(), ' Direction'))
    elif "Velocity" in string or "velocity" in string:
        long_name = "".join(('Gantry velocity in ', string[9].upper(), ' Direction'))
    elif "Position" in string or "position" in string:
        long_name = "".join(('Position in ', string[9].upper(), ' Direction'))
    else:
        long_name = _reformat_string(string)

    if 'Position' in string or 'position' in string:
        return _reformat_string(string),\
               {
                   "units"    : _UNIT_DICTIONARY[string[string.find('[') + 1:string.find(']')]],
                   "long_name": long_name
               }
    elif 'Velocity' in string or 'velocity' in string:
        return _reformat_string(string),\
               {
                   "units"    : _UNIT_DICTIONARY[string[string.find('[') + 1:string.find(']')]],
                   "long_name": long_name
               }
    elif 'speed' in string and "current setting" not in string:
        return _reformat_string(string),\
               {
                   "units"    : _UNIT_DICTIONARY[string[string.find('[') + 1:string.find(']')]],
                   "long_name": long_name
               }
    else:
        return long_name,\
               {
                   "long_name": _reformat_string(string) 
               }
               

def _filter_the_headings(target):
    '''
    A hook for json module to filter and process the useful data
    '''
    if u'lemnatec_measurement_metadata' in target:
        return DataContainer(target)
    return target


def jsonHandler(jsonFile, _debug=True):
    '''
    pass the json object to built-in json module
    '''
    with open("".join((jsonFile[:-4],'_metadata.json'))) as fileHandler:
        if _debug:
            jsonCheck(fileHandler)
        return json.loads(fileHandler.read(), object_hook=_filter_the_headings)

def translate_time(yearMonthDate, frameTimeString=None):
    hourUnpack, timeUnpack = None, None
    _unix_basetime    = date(year=1970, month=1, day=1)
    time_pattern      = re.compile(r'(\d{4})-(\d{2})-(\d{2})'),\
                        re.compile(r'(\d{2})/(\d{2})/(\d{4})\s(\d{2}):(\d{2}):(\d{2})'),\
                        re.compile(r'(\d{2}):(\d{2}):(\d{2})')

    if frameTimeString:
        hourUnpack = datetime.strptime(frameTimeString, "%H:%M:%S").timetuple()
    
    if time_pattern[1].match(yearMonthDate):
        timeUnpack = datetime.strptime(yearMonthDate, "%m/%d/%Y %H:%M:%S").timetuple()
    elif time_pattern[0].match(yearMonthDate):
        timeUnpack = datetime.strptime(yearMonthDate, "%Y-%m-%d").timetuple()

    timeSplit  = date(year=timeUnpack.tm_year, month=timeUnpack.tm_mon,
                      day=timeUnpack.tm_mday) - _unix_basetime #time period to the UNIX basetime
    if frameTimeString:
        return (timeSplit.total_seconds() + hourUnpack.tm_hour * 3600.0 + hourUnpack.tm_min * 60.0 +
                hourUnpack.tm_sec) / (3600.0 * 24.0)
    else:
        return timeSplit.total_seconds() / (3600.0 * 24.0)

def frame_index_parser(fileName, yearMonthDate):
    '''
    translate all the time in *frameIndex.txt
    '''
    with open(fileName) as fileHandler:
        return [translate_time(yearMonthDate, dataMembers.split()[1]) for dataMembers in fileHandler.readlines()[1:]]


def file_dependency_check(filePath):
    '''
    Check if the input location has all 
    '''
    filename_base = os.path.basename(filePath)
    
    all_files = {filename_base       : False,
                 filename_base+".hdr": False,

                 filename_base[:-4]+"_metadata.json" : False,
                 filename_base[:-4]+"_frameIndex.txt": False}

    for roots, directorys, files in os.walk(filePath.rstrip(os.path.split(filePath)[-1])):
        for file in files:
            if file in all_files:
                all_files[file] = True

    return [missing_file for missing_file in all_files if not all_files[missing_file]]

def jsonCheck(fileHandler):
    cache = list()
    for data in fileHandler.readlines():
        if ':' in data:
            if data.split(':')[0].strip() in cache:
                print >> sys.stderr, _WARN_MSG.format(msg='WARNING: Duplicate keys mapped to different values; such illegal mapping may cause data loss')
                print >> sys.stderr, ''.join(('Duplicated key is ', data.split(':')[0].strip(), ' in file ', fileHandler.name))
            cache.append(data.split(':')[0].strip())

    fileHandler.seek(0) #Reset the file read ptr


def write_header_file(fileName, netCDFHandler, flatten=False, _debug=True):
    '''
    The main function, reading the data and exporting netCDF file
    '''
    if not getDimension(fileName, _debug):
        print >> sys.stderr, "ERROR: Cannot get dimension infos from", "".join((fileName, '.hdr'))
        return
    dimensionWavelength, dimensionX, dimensionY = getDimension(fileName)
    hdrInfo = get_header_info(fileName)

    # netCDFHandler.createDimension('wavelength',       dimensionWavelength)
    # netCDFHandler.createDimension('x',          dimensionX)
    # netCDFHandler.createDimension('y',          dimensionY)
    # netCDFHandler.createDimension('wavelength', len(wavelength))

    # mainDataHandler, tempVariable = open(fileName + '_raw'), netCDFHandler.createVariable(
    #     'exposure_2', 'f8', ('band', 'x', 'y'))  # ('band', 'x', 'y')
    # fileSize = os.path.getsize(fileName)
    # dataNumber, dataType, dataSize = fileSize / DATATYPE[hdrInfo['data type']][-1], DATATYPE[hdrInfo['data type']][0],\
    #     DATATYPE[hdrInfo['data type']][-1]

    # with TimeMeasurement("unpacking") as lineTiming: #measuring the time
    # value =
    # struct.unpack(dataType*dataNumber,mainDataHandler.read(dataSize*dataNumber))#reading
    # the data from the file

    # with TimeMeasurement("assigning value") as lineTiming:
    # tempVariable[:,:,:] = value

    #setattr(netCDFHandler, 'wavelength', wavelength)
    headerInfo = netCDFHandler.createGroup("header_info") if not flatten else netCDFHandler
    threeColorBands = list()

    for members in hdrInfo:
        if members == 'default bands':
            threeColorBands = [int(bands) for bands in eval(hdrInfo[members])]
        setattr(headerInfo, _reformat_string(members), hdrInfo[members])

    try:
        headerInfo.createVariable('red_band_index', 'u2')[...]   = threeColorBands[0]
        setattr(netCDFHandler.groups['header_info'].variables['red_band_index'],
                'long_name', 'Index of red band used for RGB composite')

        headerInfo.createVariable('green_band_index', 'u2')[...] = threeColorBands[1]
        setattr(netCDFHandler.groups['header_info'].variables['green_band_index'],
                'long_name', 'Index of green band used for RGB composite')

        headerInfo.createVariable('blue_band_index', 'u2')[...]  = threeColorBands[2]
        setattr(netCDFHandler.groups['header_info'].variables['blue_band_index'],
                'long_name', 'Index of blue band used for RGB composite')

        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'red_band_index',   threeColorBands[0])
        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'green_band_index', threeColorBands[1])
        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'blue_band_index',  threeColorBands[2])
        # blue_band_index long_name = 'Index of blue band used for RGB composite'
        
    except:
        pass

def _argument_parser(*args):
    assert len(args) >= 3, "Please make sure you have enough arguments! (sourcefile, [debug_option,], [format_option], [flatten_option], fileInput, fileoutput)"

    source   = args[0]
    input_f  = args[-2]
    output_f = args[-1]
    format   = 4
    flatten  = "yes"
    debug    = "yes"

    format_regex  = r"fmt=(3|4)"
    debug_regex   = r"dbg=(yes|no)"
    flatten_regex = r"ftn=(yes|no)"

    for members in args:
        if re.match(format_regex, members):
            format  = int(re.match(format_regex, members).groups(1)[0])
        elif re.match(flatten_regex, members):
            flatten = re.match(flatten_regex, members).groups(1)[0]
        elif re.match(debug_regex, members):
            debug = re.match(debug_regex, members).groups(1)[0]
          
    flatten = True if flatten == "yes" else False
    flatten = True if format == 3 else flatten
    debug   = True if debug == "yes" else False
    format  = "NETCDF4" if format == 4 else "NETCDF3_CLASSIC"

    return source, input_f, output_f, format, flatten, debug


def main():
    source_file, file_input, file_output, format, flatten, debug = _argument_parser(*sys.argv[1:])

    missing_files = file_dependency_check(file_input)

    if len(missing_files) > 0:
        print >> sys.stderr, _WARN_MSG.format(msg="One or more important file(s) is(are) missing. Program terminated")

        for missing_file in missing_files:
            print >> sys.stderr, "".join((missing_file," is missing"))
        exit()

    testCase = jsonHandler(file_input, debug)
    testCase.writeToNetCDF(file_input, file_output, " ".join((file_input, file_output)), format, flatten, debug)


if __name__ == '__main__':
    main()

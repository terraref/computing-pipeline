#!/usr/bin/env python

'''
Created on Feb 5, 2016
This module parses JSON formatted metadata and data and header provided by LemnaTec and outputs a formatted netCDF4 file

@author: jeromemao
----------------------------------------------------------------------------------------
This script works with both Python 2.7+ and 3+, depending on the netCDF4 module version
Thanks for the advice from Professor Zender and sample data from Dr. LeBauer.
----------------------------------------------------------------------------------------
Usage (commandline):
python hyperspectral_metadata.py filePath1 filePath2

where
hyperspectral_metadata.py is where this script located
filePath1      is source data file
filePath2      is user's desired output file

Warning:
Make sure the json metadata ended with <data_name>+_metadata.json and the hdr file ended with <data_name>+_raw.hdr
For example, if you have a group of data named like this:

Data: data_raw
Metadata: data_metadata.json
Header: data_raw.hdr

The correct command is
python ${HOME}/terraref/computing-pipeline/scripts/hyperspectral/hyperspectral_metadata.py ${DATA}/terraref/data_raw ${DATA}/terraref/output

hyperspectral_metadata.py will authomatically find data_raw, data_metadata.json and data_raw.hdr

Example:
python ${HOME}/terraref/computing-pipeline/scripts/hyperspectral/hyperspectral_metadata.py ${DATA}/terraref/test_metadata.json ${DATA}/terraref/data
----------------------------------------------------------------------------------------
UPDATE LOG (reverse chronological order)

Update 20160901:
Rename from JsonDealer.py to hyperspectral_metadata.py

Update 20160822:
Fix major bugs, including:
1. file checking functions now works as expectedly by reimplemented with regular expression
2. the data from "user_given_metadata" are saved as group attributes except those time variables
3. translateTime() now could calculate either time since Unix base time or the time split between certain time points
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
from netCDF4 import Dataset
from hyperspectral_calculation import pixel2Geographic, REFERENCE_POINT

_UNIT_DICTIONARY = {'m': 'meter',
                    's': 'second', 'm/s': 'meter second-1', '': ''}

_VELOCITY_DICTIONARY = {'x': 'u', 'y': 'v', 'z': 'w'}

DATATYPE = {'1': ('H', 2), '2': ('i', 4), '3': ('l', 4), '4': ('f', 4), '5': (
    'd', 8), '12': ('H', 4), '13': ('L', 4), '14': ('q', 8), '15': ('Q', 8)}

_UNIX_BASETIME    = date(year=1970, month=1, day=1)

_FILENAME_PATTERN = r'^(\S+)_(\w{3,10})[.](\w{3,4})$'

_TIME_PATTERN     = re.compile(r'(\d{4})-(\d{2})-(\d{2})'),\
                    re.compile(r'(\d{2})/(\d{2})/(\d{4})\s(\d{2}):(\d{2}):(\d{2})'),\
                    re.compile(r'(\d{2}):(\d{2}):(\d{2})')

_CAMERA_POSITION  = np.array([1.9, 0.855, 0.635])

_IS_DIGIT         = lambda fakeNum: set([member.isdigit() for member in fakeNum.split(".")]) == {True}

_TIMESTAMP        = lambda: time.strftime("%a %b %d %H:%M:%S %Y",  time.localtime(int(time.time())))


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

    def writeToNetCDF(self, inputFilePath, outputFilePath, commandLine):
        # weird, but useful to check whether the HeaderInfo id in the netCDF
        # file
        setattr(self, "header_info", None)
        netCDFHandler = _fileExistingCheck(outputFilePath, self)
        delattr(self, "header_info")
        print '\033[0;31mProcessing ...\033[0m'

        #### Replace the original isdigit function

        ##### Write the data from metadata to netCDF #####
        for members in self.__dict__:
            tempGroup = netCDFHandler.createGroup(members)
            for submembers in self.__dict__[members]:
                if not _IS_DIGIT(self.__dict__[members][submembers]): #Case for letter variables
                    if 'date' in submembers and submembers != "date of installation" and submembers != "date of handover":
                        tempVariable = tempGroup.createVariable(_replaceIllegalChar(submembers), 'f8')
                        tempVariable[...] = translateTime(self.__dict__[members][submembers])
                        setattr(tempVariable, "units",     "days since 1970-01-01 00:00:00")
                        setattr(tempVariable, "calender", "gregorian")

                    setattr(tempGroup, _replaceIllegalChar(submembers),
                            self.__dict__[members][submembers])

                else: #Case for digits variables
                    if "time" in self.__dict__[members]:
                        yearMonthDate = self.__dict__[members]["time"]
                    elif "Time" in self.__dict__[members]:
                        yearMonthDate = self.__dict__[members]["Time"]
                    setattr(tempGroup, _replaceIllegalChar(submembers),
                            self.__dict__[members][submembers])
                    nameSet = _spliter(submembers)

                    if 'Velocity' in submembers or 'Position' in submembers:
                        tempVariable = tempGroup.createVariable(
                            nameSet[0][-1], 'f8')
                        setattr(tempVariable, 'long_name', nameSet[0][0])
                        setattr(tempVariable, 'units',      nameSet[1])
                    else:
                        tempVariable = tempGroup.createVariable(
                            nameSet[0], 'f8')
                        setattr(tempVariable, 'long_name', nameSet[0])

                    tempVariable[...] = float(self.__dict__[members][submembers])

        ##### Write the data from header files to netCDF #####
        wavelength = getWavelength(inputFilePath)
        netCDFHandler.createDimension("wavelength", len(wavelength))
        tempWavelength = netCDFHandler.createVariable(
            "wavelength", 'f8', 'wavelength')
        setattr(tempWavelength, 'long_name', 'Hyperspectral Wavelength')
        setattr(tempWavelength, 'units', 'nanometers')
        tempWavelength[:] = wavelength
        writeHeaderFile(inputFilePath, netCDFHandler)

        ##### Write the data from frameIndex files to netCDF #####
        tempFrameTime = frameIndexParser(''.join((inputFilePath.strip("raw"), "frameIndex.txt")), yearMonthDate)
        netCDFHandler.createDimension("time", len(tempFrameTime))
        frameTime    = netCDFHandler.createVariable("frametime", "f8", ("time",))
        frameTime[:] = tempFrameTime
        setattr(frameTime, "units",     "days since 1970-01-01 00:00:00")
        setattr(frameTime, "calender", "gregorian")

        ########################### Adding geographic positions ###########################

        xPixelsLocation, yPixelsLocation, boundingBox, googleMapAddress\
         = pixel2Geographic("".join((inputFilePath[:-4],"_metadata.json")), "".join((inputFilePath,'.hdr')))

        netCDFHandler.createDimension("x", len(xPixelsLocation))
        x    = netCDFHandler.createVariable("x", "f8", ("x",))
        x[:] = xPixelsLocation
        setattr(netCDFHandler.variables["x"], "units", "meters")
        setattr(netCDFHandler.variables['x'], 'reference_point', 'Southeast corner of the field')
        setattr(netCDFHandler.variables['x'], "long_name", "Real world X coordinates for each pixel")

        netCDFHandler.createDimension("y", len(yPixelsLocation))
        y    = netCDFHandler.createVariable("y", "f8", ("y",))
        y[:] = yPixelsLocation
        setattr(netCDFHandler.variables["y"], "units", "meters")
        setattr(netCDFHandler.variables['y'], 'reference_point', 'Southeast corner of the field')
        setattr(netCDFHandler.variables['y'], "long_name", "Real world Y coordinates for each pixel")

        x_pt, y_pt = REFERENCE_POINT

        x_ref_pt = netCDFHandler.createVariable("x_reference_point", "f8")
        x_ref_pt[...] = x_pt
        setattr(netCDFHandler.variables["x_reference_point"], "units", "degrees")
        setattr(netCDFHandler.variables["x_reference_point"], "long_name", "The overall reference point in the field, at southeast corner")
        setattr(netCDFHandler.variables["x_reference_point"], "provenance", "https://github.com/terraref/reference-data/issues/32 by Dr. David LeBauer")

        y_ref_pt = netCDFHandler.createVariable("y_reference_point", "f8")
        y_ref_pt[...] = y_pt
        setattr(netCDFHandler.variables["y_reference_point"], "units", "degrees")
        setattr(netCDFHandler.variables["y_reference_point"], "long_name", "The overall reference point in the field, at southeast corner")
        setattr(netCDFHandler.variables["y_reference_point"], "provenance", "https://github.com/terraref/reference-data/issues/32 by Dr. David LeBauer")

        #write the latitude and longitude of the bounding box
        SE, SW, NE, NW = boundingBox[0], boundingBox[1], boundingBox[2], boundingBox[3]
        lat_se, lng_se = tuple(SE.split(", "))
        lat_sw, lng_sw = tuple(SW.split(", "))
        lat_ne, lng_ne = tuple(NE.split(", "))
        lat_nw, lng_nw = tuple(NW.split(", "))

        latSe = netCDFHandler.createVariable("lat_img_southeast_corner", "f8")
        latSe[...] = float(lat_se)
        setattr(netCDFHandler.variables["lat_img_southeast_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lat_img_southeast_corner"], "long_name", "Langtitude of the southeast corner of the picture")

        # have a "x_y_img_southeast_corner" in meters, double
        lonSe = netCDFHandler.createVariable("lng_img_southeast_corner", "f8")
        lonSe[...] = float(lng_se)
        setattr(netCDFHandler.variables["lng_img_southeast_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lng_img_southeast_corner"], "long_name", "Longitude of the southeast corner of the picture")

        latSw = netCDFHandler.createVariable("lat_img_southwest_corner", "f8")
        latSw[...] = float(lat_sw)
        setattr(netCDFHandler.variables["lat_img_southwest_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lat_img_southwest_corner"], "long_name", "Langtitude of the southwest corner of the picture")

        lonSw = netCDFHandler.createVariable("lng_img_southwest_corner", "f8")
        lonSw[...] = float(lng_sw)
        setattr(netCDFHandler.variables["lng_img_southwest_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lng_img_southwest_corner"], "long_name", "Longitude of the southwest corner of the picture")

        latNe = netCDFHandler.createVariable("lat_img_northeast_corner", "f8")
        latNe[...] = float(lat_ne)
        setattr(netCDFHandler.variables["lat_img_northeast_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lat_img_northeast_corner"], "long_name", "Langtitude of the northeast corner of the picture")

        lngNe = netCDFHandler.createVariable("lng_img_northeast_corner", "f8")
        lngNe[...] = float(lng_ne)
        setattr(netCDFHandler.variables["lng_img_northeast_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lng_img_northeast_corner"], "long_name", "Longitude of the northeast corner of the picture")

        latNw = netCDFHandler.createVariable("lat_img_northwest_corner", "f8")
        latNw[...] = float(lat_nw)
        setattr(netCDFHandler.variables["lat_img_northwest_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lat_img_northwest_corner"], "long_name", "Langtitude of the northwest corner of the picture")

        lngNw = netCDFHandler.createVariable("lng_img_northwest_corner", "f8")
        lngNw[...] = float(lng_nw)
        setattr(netCDFHandler.variables["lng_img_northwest_corner"], "units", "degrees")
        setattr(netCDFHandler.variables["lng_img_northwest_corner"], "long_name", "Longitude of the northwest corner of the picture")

        xSe = netCDFHandler.createVariable("x_img_southeast_corner", "f8")
        xSe[...] = float(x[-1] + REFERENCE_POINT[0])
        setattr(netCDFHandler.variables["x_img_southeast_corner"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_southeast_corner"], "long_name", "meters of the southeast corner of the picture")

        # have a "x_y_img_southeast_corner" in meters, double
        ySe = netCDFHandler.createVariable("y_img_southeast_corner", "f8")
        ySe[...] = float(y[-1] + REFERENCE_POINT[1])
        setattr(netCDFHandler.variables["y_img_southeast_corner"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_southeast_corner"], "long_name", "Longitude of the southeast corner of the picture")

        xSw = netCDFHandler.createVariable("x_img_southwest_corner", "f8")
        xSw[...] = float(x[0] + REFERENCE_POINT[0])
        setattr(netCDFHandler.variables["x_img_southwest_corner"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_southwest_corner"], "long_name", "Langtitude of the southwest corner of the picture")

        ySw = netCDFHandler.createVariable("y_img_southwest_corner", "f8")
        ySw[...] = float(y[-1] + REFERENCE_POINT[1])
        setattr(netCDFHandler.variables["y_img_southwest_corner"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_southwest_corner"], "long_name", "Longitude of the southwest corner of the picture")

        xNe = netCDFHandler.createVariable("x_img_northeast_corner", "f8")
        xNe[...] = float(x[-1] + REFERENCE_POINT[0])
        setattr(netCDFHandler.variables["x_img_northeast_corner"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_northeast_corner"], "long_name", "Langtitude of the northeast corner of the picture")

        yNe = netCDFHandler.createVariable("y_img_northeast_corner", "f8")
        yNe[...] = float(y[0] + REFERENCE_POINT[1])
        setattr(netCDFHandler.variables["y_img_northeast_corner"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_northeast_corner"], "long_name", "Longitude of the northeast corner of the picture")

        xNw = netCDFHandler.createVariable("x_img_northwest_corner", "f8")
        xNw[...] = float(x[0] + REFERENCE_POINT[0])
        setattr(netCDFHandler.variables["x_img_northwest_corner"], "units", "meters")
        setattr(netCDFHandler.variables["x_img_northwest_corner"], "long_name", "Langtitude of the northwest corner of the picture")

        yNw = netCDFHandler.createVariable("y_img_northwest_corner", "f8")
        yNw[...] = float(y[0] + REFERENCE_POINT[1])
        setattr(netCDFHandler.variables["y_img_northwest_corner"], "units", "meters")
        setattr(netCDFHandler.variables["y_img_northwest_corner"], "long_name", "Longitude of the northwest corner of the picture")

        googleMapView = netCDFHandler.createVariable("Google_Map_View", str)
        googleMapView[...] = googleMapAddress
        setattr(netCDFHandler.variables["Google_Map_View"], "usage", "copy and paste to your web browser")
        setattr(netCDFHandler.variables["Google_Map_View"], 'reference_point', 'Southeast corner of the field')

        ##### Write the history to netCDF #####
        netCDFHandler.history = ''.join((_TIMESTAMP(), ': python ', commandLine))

        netCDFHandler.close()


def getDimension(fileName):
    '''
    Acquire dimensions from related HDR file
    '''
    fileHandler = open("".join((fileName, '.hdr')))

    for members in fileHandler.readlines():
        if "samples" == members[:7]:
            x = members[members.find("=") + 1:len(members)]
        elif "lines" == members[:5]:
            y = members[members.find("=") + 1:len(members)]
        elif "bands" == members[:5]:
            wavelength = members[members.find("=") + 1:len(members)]

    fileHandler.close()

    try:
        return int(wavelength.strip('\n').strip('\r')), int(x.strip('\n').strip('\r')), int(y.strip('\n').strip('\r'))
    except:
        print >> sys.stderr, 'Fatal Warning: sample, lines and bands variables in header file are broken. Header information will not be written into the netCDF'
        return 0, 0

def getWavelength(fileName):
    '''
    Acquire wavelength(s) from related HDR file
    '''
    with open("".join((fileName, '.hdr'))) as fileHandler:
        wavelengthGroup = [float(x.strip('\r').strip('\n').strip(',')) for x in fileHandler.readlines() if _IS_DIGIT(x.strip('\r').strip('\n').strip(','))]
    return wavelengthGroup


def getHeaderInfo(fileName):
    '''
    Acquire Other Information from related HDR file
    '''
    with open("".join((fileName, '.hdr'))) as fileHandler:
        infoDictionary = {members[0:members.find("=") - 1].strip(";") : members[members.find("=") + 2:].strip('\n').strip('\r') for members in fileHandler.readlines() if '=' in members and 'wavelength' not in members}
        return infoDictionary


def _fileExistingCheck(filePath, dataContainer):
    '''
    This method will check wheter the filePath has the same variable name as the dataContainer has. If so,
    user will decide whether skip or overwrite it (no append,
    since netCDF does not support the repeating variable names)
    '''
    userPrompt = '\033[0;31m--> Output file already exists; skip it or overwrite or append? (\033[4;31mS\033[0;31mkip, \033[4;31mO\033[0;31mverwrite, \033[4;31mA\033[0;31mppend\033[0m)'

    if os.path.isdir(filePath):
        filePath += ("/" + filePath.split("/")[-1] + ".nc")

    if os.path.exists(filePath):
        netCDFHandler = Dataset(filePath, 'r', format='NETCDF4')
        if set([x.encode('utf-8') for x in netCDFHandler.groups]) - \
           set([x for x in dataContainer.__dict__]) != set([x.encode('utf-8') for x in netCDFHandler.groups]):

            while True:
                userChoice = str(raw_input(userPrompt))

                if userChoice is 'S':
                    print "Exit due to the skipping"
                    exit()
                elif userChoice in ('O', 'A'):
                    os.remove(filePath)
                    return Dataset(filePath, 'w', format='NETCDF4')
        else:
            os.remove(filePath)

    return Dataset(filePath, 'w', format='NETCDF4')

def _replaceIllegalChar(string):
    '''
    This method will replace spaces (' '), slashes('/') and many other unwanted characters
    '''
    if "current setting" in string:
        string = string.split(' ')[-1]
    elif "Velocity" in string:
        string = "".join(('Gantry Speed in ', string[-1].upper(), ' Direction'))
    elif "Position" in string:
        string = "".join(('Position in ', string[-1].upper(), ' Direction'))

    string = string.replace('/', '_per_')
    string = string.replace(' ', '_')

    if '(' in string:
        return string[:string.find('(') - 1]
    elif '[' in string:
        return string[:string.find('[') - 1]

    return string


def _spliter(string):
    '''
    This method will parse the string to a group of long names, short names and values
    Position and Velocity variables will be specially treated
    '''
    long_name = string.replace("[","")

    if 'Position' in string:
        return [_replaceIllegalChar(long_name.strip(' ')),
                long_name.strip(' ').split(' ')[-1]]\
            , _UNIT_DICTIONARY[string[string.find('[') + 1:
                                      string.find(']')].encode('ascii', 'ignore')]

    elif 'Velocity' in string:
        return [_replaceIllegalChar(long_name.strip(' ')),
                _VELOCITY_DICTIONARY[long_name.strip(' ').split(' ')[-1]]]\
            , _UNIT_DICTIONARY[string[string.find('[') + 1:
                                      string.find(']')].encode('ascii', 'ignore')]

    else:
        return _replaceIllegalChar(long_name.strip(' '))\
            , _replaceIllegalChar(string)


def jsonHandler(jsonFile):
    '''
    pass the json object to built-in json module
    '''
    with open("".join((jsonFile[:-4],'_metadata.json'))) as fileHandler:
        jsonCheck(fileHandler)
        return json.loads(fileHandler.read(), object_hook=_filteringTheHeadings)


def translateTime(yearMonthDate, frameTimeString=None):
    hourUnpack, timeUnpack = None, None

    if frameTimeString:
        hourUnpack = datetime.strptime(frameTimeString, "%H:%M:%S").timetuple()
    
    if _TIME_PATTERN[1].match(yearMonthDate):
        timeUnpack = datetime.strptime(yearMonthDate, "%m/%d/%Y %H:%M:%S").timetuple()
    elif _TIME_PATTERN[0].match(yearMonthDate):
        timeUnpack = datetime.strptime(yearMonthDate, "%Y-%m-%d").timetuple()

    timeSplit  = date(year=timeUnpack.tm_year, month=timeUnpack.tm_mon,
                      day=timeUnpack.tm_mday) - _UNIX_BASETIME
    if frameTimeString:
        return (timeSplit.total_seconds() + hourUnpack.tm_hour * 3600.0 + hourUnpack.tm_min * 60.0 +
                hourUnpack.tm_sec) / (3600.0 * 24.0)
    else:
        return timeSplit.total_seconds() / (3600.0 * 24.0)


def frameIndexParser(fileName, yearMonthDate):
    with open(fileName) as fileHandler:
        return [translateTime(yearMonthDate, dataMembers.split()[1]) for dataMembers in fileHandler.readlines()[1:]]


def _filteringTheHeadings(target):
    '''
    A hook for json module to filter and process the useful data
    '''
    if u'lemnatec_measurement_metadata' in target:
        return DataContainer(target)
    return target


def fileDependencyCheck(filePath):
    '''
    Check if the input location has all 
    '''
    key = str()
    illegalFileRegex = re.compile(_FILENAME_PATTERN)
    for roots, directorys, files in os.walk(filePath.rstrip(os.path.split(filePath)[-1])):
        for file in files:
            if re.match(_FILENAME_PATTERN, file):
                key = illegalFileRegex.match(file).group(1)
                return {"".join((key,"_frameIndex.txt")), "".join((key,"_metadata.json")), "".join((key,"_raw.hdr"))} -\
                        set([matchFile for matchFile in files if matchFile.startswith(illegalFileRegex.match(file).group(1))])

def jsonCheck(fileHandler):
    cache = list()
    for data in fileHandler.readlines():
        if ':' in data:
            if data.split(':')[0].strip() in cache:
                print >> sys.stderr, '\033[0;31m--> Warning: Multiple keys are mapped to a single value; such illegal mapping may cause the loss of important data.\033[0m'
                print >> sys.stderr, ''.join(('\033[0;31m--> The file path is ', fileHandler.name, ', and the key is ', data.split(':')[0].strip(), '\033[0m'))
            cache.append(data.split(':')[0].strip())

    fileHandler.seek(0) #Reset the file read ptr

def writeHeaderFile(fileName, netCDFHandler):
    '''
    The main function, reading the data and exporting netCDF file
    '''
    if not getDimension(fileName):
        print >> sys.stderr, "\033[0;31mError: Cannot get dimension infos from", "".join((fileName, '.hdr')), "\033[0m"
        return
    dimensionWavelength, dimensionX, dimensionY = getDimension(fileName)
    hdrInfo = getHeaderInfo(fileName)

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
    headerInfo = netCDFHandler.createGroup("header_info")
    threeColorBands = list()

    for members in hdrInfo:
        if members == 'default bands':
            threeColorBands = [int(bands) for bands in eval(hdrInfo[members])]
        setattr(headerInfo, _replaceIllegalChar(members), hdrInfo[members])

    try:
        headerInfo.createVariable(
            'red_band_index', 'f8')[...] = threeColorBands[0]
        headerInfo.createVariable(
            'green_band_index', 'f8')[...] = threeColorBands[1]
        headerInfo.createVariable(
            'blue_band_index', 'f8')[...] = threeColorBands[2]

        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'red_band_index',   threeColorBands[0])
        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'green_band_index', threeColorBands[1])
        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'blue_band_index',  threeColorBands[2])
    except:
        print >> sys.stderr, '\033[0;31mWarning: default_band variable in the header file is missing.\033[0m'

def main():
    fileInput, fileOutput = sys.argv[1], sys.argv[2]
    missingFiles = fileDependencyCheck(fileInput)
    if len(missingFiles) > 0:
        print >> sys.stderr, "\033[0;31mOne or more important file(s) is(are) missing. Program terminated:\033[0m"

        for missingFile in missingFiles:
            print >> sys.stderr, "".join(("\033[0;31m",missingFile," is missing\033[0m"))
        exit()

    testCase = jsonHandler(fileInput)
    testCase.writeToNetCDF(fileInput, fileOutput, " ".join((fileInput, fileOutput)))
    print '\033[0;31mDone.\033[0m'


if __name__ == '__main__':
    main()

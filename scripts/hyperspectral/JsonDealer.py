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
python JsonDealerPath filePath1 filePath2

where
JsonDealerPath is where this script located
filePath1      is where the source data file located <data_name>
filePath2      is users' expected output location

Warning:
Make sure the json metadata ended with <data_name>+_metadata.json and the hdr file ended with <data_name>+_raw.hdr
For example, if you have a group of data named like this:

Data: data_raw
Metadata: data_metadata.json
Header: data_raw.hdr

You just need to type in 
python ${HOME}/terraref/computing-pipeline/scripts/hyperspectral/JsonDealer.py ${DATA}/terraref/data_raw ${DATA}/terraref/output

JsonDealer will authomatically find data_raw, data_metadata.json and data_raw.hdr for you

Example:
python ${HOME}/terraref/computing-pipeline/scripts/hyperspectral/JsonDealer.py ${DATA}/terraref/test_metadata.json ${DATA}/terraref/data
----------------------------------------------------------------------------------------
UPDATE LOG

Update 4.1:
Merged with DataProcess module; now JsonDealer will do all the jobs.

Update 4.12:
Fixed bugs in getting dimensions from the header file.

Update 4.25:
Attributes and variables now looks nicer.
Rename "Velocity ..." as "Gantry Speed ..."
Set the "default bands" variable from the header file as attributes of "exposure" variable.

Update 5.9:
Now the JsonDealer.py will also parse the data from frameIndex.txt
the time-related variable (except history) will be recorded as the offset to the _UNIX_BASETIME

Update 8.22:
Fix major bugs, including:
1. file checking functions now works as expectedly by reimplemented with regular expression
2. the data from "user_given_metadata" are saved as group attributes except those time variables
3. translateTime() now could calculate either time since Unix base time or the time split between certain time points
Other improvements including better implementations on DataContainer and a more friendly prompts to users
when the output file had already existed.

JsonDealer widely uses regular expressions to match string; although most of them are compatible
with Java, PHP, Perl, etc., some of the regular expressions are only supported by the Python standard.
----------------------------------------------------------------------------------------
'''
import numpy as np
import json
import time
import sys
import os
import re
import platform
import struct
from datetime import date, datetime
from netCDF4 import Dataset

_UNIT_DICTIONARY = {'m': 'meter',
                    's': 'second', 'm/s': 'meter second-1', '': ''}
_VELOCITY_DICTIONARY = {'x': 'u', 'y': 'v', 'z': 'w'}
DATATYPE = {'1': ('H', 2), '2': ('i', 4), '3': ('l', 4), '4': ('f', 4), '5': (
    'd', 8), '12': ('H', 4), '13': ('L', 4), '14': ('q', 8), '15': ('Q', 8)}
_RAW_VERSION      = platform.python_version()[0]
_UNIX_BASETIME    = date(year=1970, month=1, day=1)
_FILENAME_PATTERN = r'^(\S+)_(\w{3,10})[.](\w{3,4})$'
_TIME_PATTERN     = re.compile(r'(\d{4})-(\d{2})-(\d{2})'), re.compile(r'(\d{2})/(\d{2})/(\d{4})\s(\d{2}):(\d{2}):(\d{2})'), re.compile(r'(\d{2}):(\d{2}):(\d{2})')
_CAMERA_POSITION  = np.array([1.9, 0.855, 0.635])


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
        printOnVersion('\033[0;31mProcessing ...\033[0m')

        if not netCDFHandler:
            return

        ##### Write the data from metadata to netCDF #####
        for members in self.__dict__:
            tempGroup = netCDFHandler.createGroup(members)
            for submembers in self.__dict__[members]:
                if not isDigit(self.__dict__[members][submembers]): #Case for letter variables
                    if 'date' in submembers:
                        tempVariable = tempGroup.createVariable(_replaceIllegalChar(submembers), 'f8')
                        tempVariable.assignValue(translateTime(self.__dict__[members][submembers]))
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

                    tempVariable.assignValue(
                        float(self.__dict__[members][submembers]))

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
        netCDFHandler.createDimension("y", len(tempFrameTime))
        frameTime    = netCDFHandler.createVariable("frametime", "f8", ("y",))
        frameTime[:] = tempFrameTime
        setattr(frameTime, "units",     "days since 1970-01-01 00:00:00")
        setattr(frameTime, "calender", "gregorian")


        ##### Write the history to netCDF #####
        netCDFHandler.history = ''.join((_timeStamp(), ': python ', commandLine))

        netCDFHandler.close()


def getDimension(fileName):
    '''
    Acquire dimensions from related HDR file
    '''
    fileHandler = open(fileName + '.hdr')

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
        printOnVersion('Fatal Warning: sample, lines and bands variables in header file are broken. Header information will not be written into the netCDF')


def getWavelength(fileName):
    '''
    Acquire wavelength(s) from related HDR file
    '''
    with open(fileName + '.hdr') as fileHandler:
        wavelengthGroup = [float(x.strip('\r').strip('\n').strip(',')) for x in fileHandler.readlines()
                           if isDigit(x.strip('\r').strip('\n').strip(','))]
    return wavelengthGroup


def getHeaderInfo(fileName):
    '''
    Acquire Other Information from related HDR file
    '''
    with open(fileName + '.hdr') as fileHandler:
        infoDictionary = {members[0:members.find("=") - 1].strip(";"): members[members.find("=") + 2:].strip('\n').strip('\r')
                          for members in fileHandler.readlines() if '=' in members and 'wavelength' not in members}

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
                if _RAW_VERSION == '2':
                    exec("userChoice = str(raw_input(userPrompt))")
                else:
                    exec("userChoice = str(input(userPrompt))")

                if userChoice is 'S':
                    return 0
                elif userChoice is 'O' or 'A':
                    os.remove(filePath)
                    return Dataset(filePath, 'w', format='NETCDF4')
        else:
            os.remove(filePath)
            return Dataset(filePath, 'w', format='NETCDF4')

    else:
        return Dataset(filePath, 'w', format='NETCDF4')


def isDigit(string):
    '''
    This method will check whether the string can be convert to int or float
    Similar to .isdight method in built-in string class, but python's will not check whether it is a float
    '''
    try:
        if '.' in string:
            float(string)
        else:
            int(string)
        return True
    except:
        return False


def _replaceIllegalChar(string):
    '''
    This method will replace spaces (' '), slashes('/')
    '''
    if "current setting" in string:
        string = string.split(' ')[-1]
    elif "Velocity" in string:
        string = 'Gantry Speed in ' + string[-1].upper() + ' Direction'
    elif "Position" in string:
        string = 'Position in ' + string[-1].upper() + ' Direction'

    string = string.replace('/', '_per_')
    string = string.replace(' ', '_')
    if '(' in string:
        string = string[:string.find('(') - 1]
    elif '[' in string:
        string = string[:string.find('[') - 1]

    return string


def _spliter(string):
    '''
    This method will parse the string to a group of long names, short names and values
    Position and Velocity variables will be specially treated
    '''
    long_name = str()

    for members in string:
        if members != '[':
            long_name += members
        else:
            break

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


def _filteringTheHeadings(target):
    '''
    A hook for json module to filter and process the useful data
    '''
    if u'lemnatec_measurement_metadata' in target:
        return DataContainer(target)
    return target


def _timeStamp():
    return time.strftime("%a %b %d %H:%M:%S %Y",  time.localtime(int(time.time())))


def jsonHandler(jsonFile):
    '''
    pass the json object to built-in json module
    '''
    with open(jsonFile[:-4] + '_metadata.json') as fileHandler:
        jsonCheck(fileHandler)
        return json.loads(fileHandler.read(), object_hook=_filteringTheHeadings)


def printOnVersion(prompt):
    if _RAW_VERSION == 2:
        exec("print prmpt")
    else:
        exec("print(prompt)")


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


def fileDependencyCheck(filePath):
    '''
    Check if the input location has all 
    '''
    key              = str()
    illegalFileRegex = re.compile(_FILENAME_PATTERN)
    for roots, directorys, files in os.walk(filePath.rstrip(os.path.split(filePath)[-1])):
        for file in files:
            if re.match(_FILENAME_PATTERN, file):
                key = illegalFileRegex.match(file).group(1)
                return {key+"_frameIndex.txt", key+"_metadata.json", key+"_raw.hdr"} -\
                        set([matchFile for matchFile in files if matchFile.startswith(illegalFileRegex.match(file).group(1))])

def jsonCheck(fileHandler):
    cache = list()
    for data in fileHandler.readlines():
        if ':' in data:
            if data.split(':')[0].strip() in cache:
                printOnVersion('\033[0;31m--> Warning: Multiple keys are mapped to a single value; such illegal mapping may cause the loss of important data.\033[0m')
                printOnVersion(''.join(('\033[0;31m--> The file path is ', fileHandler.name, ', and the key is ', data.split(':')[0].strip(), '\033[0m')))
            cache.append(data.split(':')[0].strip())

    fileHandler.seek(0)

def writeHeaderFile(fileName, netCDFHandler):
    '''
    The main function, reading the data and exporting netCDF file
    '''
    if not getDimension(fileName):
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
            'red_band_index', 'f8').assignValue(threeColorBands[0])
        headerInfo.createVariable(
            'green_band_index', 'f8').assignValue(threeColorBands[1])
        headerInfo.createVariable(
            'blue_band_index', 'f8').assignValue(threeColorBands[2])

        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'red_band_index',   threeColorBands[0])
        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'green_band_index', threeColorBands[1])
        setattr(netCDFHandler.groups['sensor_variable_metadata'].variables[
                'exposure'], 'blue_band_index',  threeColorBands[2])
    except:
        printOnVersion(
            'Warning: default_band variable in the header file is missing.')


if __name__ == '__main__':
    fileInput, fileOutput = sys.argv[1], sys.argv[2]
    missingFiles = fileDependencyCheck(fileInput)
    if len(missingFiles) > 0:
        printOnVersion("\033[0;31mOne or more important file(s) is(are) missing. Program terminated:\033[0m")

        for missingFile in missingFiles:
            printOnVersion("\033[0;31m" + missingFile + " is missing\033[0m")
        exit()

    testCase = jsonHandler(fileInput)
    testCase.writeToNetCDF(fileInput, fileOutput, fileInput + ' ' + fileOutput)
    printOnVersion('\033[0;31mDone.\033[0m')

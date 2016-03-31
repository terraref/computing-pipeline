#!/usr/bin/env python

'''
Created on Feb 5, 2016
This module parses JSON provided by LemnaTec and outputs a formatted netCDF4 file

@author: jeromemao
----------------------------------------------------------------------------------------
Usage (commandline):
python JsonDealerPath filePath1 filePath2

where
JsonDealerPath is where this script located
filePath1      is where the source json file located
filePath2      is users' expected output location

Example:
python ${HOME}/terraref/computing-pipeline/scripts/hyperspectral/JsonDealer.py ${DATA}/terraref/test_metadata.json ${DATA}/terraref/test_metadata.nc4
----------------------------------------------------------------------------------------
This script is version-independent, and works with both Python 2 and 3, depending on the netCDF4 module version
Thanks for the advice from Professor Zender and sample data from Dr. LeBauer.
----------------------------------------------------------------------------------------
'''

import json
import time
import sys
import os
import platform
from netCDF4 import Dataset

_constructorTemplate  = '''self.{var} = source[u'lemnatec_measurement_metadata'][u'{var}']'''
_globalUnitDictionary = {'m':'meter', 's':'second', 'm/s': 'meter second-1', '':''}
_velocityDictionary   = {'x':'u', 'y':'v', 'z':'w'}

_rawVersion = platform.python_version()[0]

class JsonError(Exception):
   '''
   User-Defined Error Class
   '''
   def __init__(self, message):
      self.message = message
        
   def __str__(self):
      return repr(self.message)

class DataContainer(object):
   '''
   A class which saves the data from Json file
   '''
   def __init__(self,source):
      for members in source[u'lemnatec_measurement_metadata']:
         exec(_constructorTemplate.format(var=members))

   def __str__(self):
      result = str()
      for members in self.__dict__:
         result += (str(members)+': '+str(self.__dict__[members])+'\n')
      return result

   def __getitem__(self, param):
      if param in self.__dict__:
         return self.__dict__[param]

   def writeToNetCDF(self, filePath, commandLine):

      netCDFHandler = _fileExistingCheck(filePath, self)

      if netCDFHandler == 0:
         return

      for members in self.__dict__:
         tempGroup = netCDFHandler.createGroup(members)
         for submembers in self.__dict__[members]:
            if not isDigit(self.__dict__[members][submembers]):
               setattr(tempGroup, _replaceIllegalChar(submembers), 
                        self.__dict__[members][submembers])

            else:
               setattr(tempGroup, _replaceIllegalChar(submembers), 
                        self.__dict__[members][submembers])
               nameSet = _spliter(submembers)

               if 'Velocity' in submembers or 'Position' in submembers:
                  tempVariable = tempGroup.createVariable(nameSet[0][-1], 'f8')
                  setattr(tempVariable,'long_name', nameSet[0][0])
                  setattr(tempVariable,'unit',      nameSet[1])
               else:
                  tempVariable = tempGroup.createVariable(nameSet[0], 'f8')
                  setattr(tempVariable,'long_name', nameSet[0])

               tempVariable.assignValue(float(self.__dict__[members][submembers]))

      netCDFHandler.history = _timeStamp()+' : python '+commandLine

      netCDFHandler.close()  


def _fileExistingCheck(filePath, dataContainer):
   '''
   This method checks whether filePath has same variable name as dataContainer. 
   If so, user decides whether skip or overwrite it (no append, since netCDF does not support the repeating variable names)

   Private to module members
   '''
   userPrompt = 'Output file already exists; skip it or overwrite or append? (S, O, A)'

   if os.path.exists(filePath):
      netCDFHandler = Dataset(filePath,'r',format='NETCDF4')
      if set([x.encode('utf-8') for x in netCDFHandler.groups]) - \
         set([x for x in dataContainer.__dict__]) != set([x.encode('utf-8') for x in netCDFHandler.groups]):

         while True:
            if _rawVersion == '2':
               exec("userChoice = str(raw_input(userPrompt))")
            else:
               exec("userChoice = str(input(userPrompt))")

            if userChoice is 'S':
               return 0
            elif userChoice is 'O' and 'A':
               os.remove(filePath)
               return Dataset(filePath,'w',format='NETCDF4')

   else:
      return Dataset(filePath,'w',format='NETCDF4')


def isDigit(string):
   '''
   This method checks whether string can convert to int or float
   Similar to .isdigit method in built-in string class, but Python's will not check whether it is a float

   Private to module members
   '''
   try:
      if '.' in string: float(string)
      else: int(string)
      return True
   except:
      return False

def _replaceIllegalChar(string):
   '''
   This method replaces spaces (' '), slashes('/')

   Private to module members
   '''
   rtn = str()
   if "current setting" in string: string = string.split(' ')[-1]

   for members in string:
      if members == '/':   rtn += ' per '
      elif members == ' ': rtn += '_'
      else:                rtn += members

   return rtn

def _spliter(string):
   '''
   This method parses the string to a group of long names, short names and values
   Position and Velocity variables will be specially treated

   Private to module members
   '''
   long_name= str()

   for members in string:
      if members != '[': long_name += members
      else: break

   if 'Position' in string:
      return [_replaceIllegalChar(long_name.strip(' ')),\
               long_name.strip(' ').split(' ')[-1]]\
               ,_globalUnitDictionary[string[string.find('[')+1: \
               string.find(']')].encode('ascii','ignore')]

   elif 'Velocity' in string:
      return [_replaceIllegalChar(long_name.strip(' ')), 
              _velocityDictionary[long_name.strip(' ').split(' ')[-1]]]\
              ,_globalUnitDictionary[string[string.find('[')+1: \
              string.find(']')].encode('ascii','ignore')]

   else:
      return _replaceIllegalChar(long_name.strip(' '))\
             ,_replaceIllegalChar(string)


def _filteringTheHeadings(target):
   '''
   Hook for JSON module to filter and process useful data

   Private to module members
   '''
   if u'lemnatec_measurement_metadata' in target:
      return DataContainer(target)
   return target

def _timeStamp():
   '''
   acquire the time from std time module and return a well-formatted timestamp
   '''
   return time.strftime("%a %b %d %H:%M:%S %Y",  time.localtime(int(time.time())))

def jsonHandler(jsonFile):
   '''
   pass JSON object to built-in JSON module
   '''
   rawData = str()

   try:
      with open(jsonFile) as fileHandler:
         for dataMember in fileHandler.readlines():
            rawData += dataMember.strip('\\').strip('\t').strip('\n')  
   except Exception as err:
      print 'Fatal Error: ', repr(err)
   return json.loads(rawData,object_hook=_filteringTheHeadings)    


if __name__ == '__main__':
   fileInput, fileOutput = sys.argv[1], sys.argv[2]

   testCase = jsonHandler(fileInput)
   testCase.writeToNetCDF(fileOutput,fileInput+' '+fileOutput)
   

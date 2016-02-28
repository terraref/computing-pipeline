'''
Created on Feb 5, 2016

@author: jeromemao
'''

import json
from netCDF4 import Dataset

_constructorTemplate  = '''self.{var} = source[u'lemnatec_measurement_metadata'][u'{var}']'''
_globalUnitDictionary = {'m':'meter', 's':'second', 'm/s': 'meters second^(-1)', '':''}
_velocityDictionary   = {'x':'u', 'y':'v', 'z':'w'}

class JsonError(Exception):
   def __init__(self, message):
      self.message = message
        
   def __str__(self):
      return repr(self.message)

class DataContainer(object):
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

   def writeToNetCDF(self):
      netCDFHandler = Dataset('test.nc','w', format='NETCDF4')

      for members in self.__dict__:
         tempGroup = netCDFHandler.createGroup(members)
         for submembers in self.__dict__[members]:
            if not _isdigit(self.__dict__[members][submembers]):
               setattr(tempGroup, _replaceIllegalChar(submembers), self.__dict__[members][submembers])
            else:
               setattr(tempGroup, _replaceIllegalChar(submembers), self.__dict__[members][submembers])
               nameSet = _spliter(submembers)

               if 'Velocity' in submembers or 'Position' in submembers:
                  tempVariable = tempGroup.createVariable(nameSet[0][-1], 'f8')
                  setattr(tempVariable,'long_name', nameSet[0][0])
                  setattr(tempVariable,'unit',      nameSet[1])
               else:
                  tempVariable = tempGroup.createVariable(nameSet[0], 'f8')
                  setattr(tempVariable,'long_name', nameSet[0])

               tempVariable.assignValue(float(self.__dict__[members][submembers]))

      netCDFHandler.close()     


def _isdigit(string):
   try:
      if '.' in string: float(string)
      else: int(string)
      return True
   except:
      return False

def _replaceIllegalChar(string):
   rtn = str()
   if "current setting" in string: string = string.split(' ')[-1]

   for members in string:
      if members == '/':   rtn += ' per '
      elif members == ' ': rtn += '_'
      else:                rtn += members

   return rtn

def _spliter(string):
   long_name= str()

   for members in string:
      if members != '[': long_name += members
      else: break

   if 'Position' in string:
      return [_replaceIllegalChar(long_name.strip(' ')), long_name.strip(' ').split(' ')[-1]], _globalUnitDictionary[string[string.find('[')+1: string.find(']')].encode('ascii','ignore')]
   elif 'Velocity' in string:
      return [_replaceIllegalChar(long_name.strip(' ')), _velocityDictionary[long_name.strip(' ').split(' ')[-1]]], _globalUnitDictionary[string[string.find('[')+1: string.find(']')].encode('ascii','ignore')]
   else:
      return _replaceIllegalChar(long_name.strip(' ')), _replaceIllegalChar(string)


def _filteringTheHeadings(target):
   '''
   A hook for json module to filter and process the useful data
   '''
   if u'lemnatec_measurement_metadata' in target:
      return DataContainer(target)
   return target

def jsonHandler(jsonFile):
   '''
   pass the json object to built-in json module
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
   testCase = jsonHandler('test.json')
   testCase.writeToNetCDF()
    
    
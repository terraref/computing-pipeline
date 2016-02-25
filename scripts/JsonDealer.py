'''
Created on Feb 5, 2016

@author: jeromemao
'''

import json
from netCDF4 import Dataset

_constructorTemplate = '''self.{var} = source[u'lemnatec_measurement_metadata'][u'{var}']'''

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
         tempGroup.createDimension(members, len(self.__dict__[members]))
         for submembers in self.__dict__[members]:
            tempValue = tempGroup.createVariable(submembers, str, members)
            tempValue = self.__dict__[members][submembers]
            #print submembers, ":", tempValue

      netCDFHandler.close()     


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
    #print len(json.loads(rawData))
   return json.loads(rawData,object_hook=_filteringTheHeadings)    

# def testReader():
#    testFile = Dataset('test.nc','r')  
#    print testFile.groups

if __name__ == '__main__':
   testCase = jsonHandler('test.json')
   testCase.writeToNetCDF()
   testReader()
    
    
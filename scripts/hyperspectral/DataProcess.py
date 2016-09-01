#!/usr/bin/env python

'''
Created on Mar 19, 2016

@author: jeromemao

This module will process the data file and export a netCDF with variables 
from it and dimesions (band, x, y) from its hdr file
----------------------------------------------------------------------------------------
Usage:
python DataProcessPath DataPath

where
DataProcessPath is where this script locates
DataPath        is where the data file locates
***make sure the header file is in the same path since the script will automatically find the
header file***

Example:
python ${HOME}/terraref/computing-pipeline/scripts/hyperspectral/DataProcess.py data
----------------------------------------------------------------------------------------
Process:
Professor Zender noticed that the image is "interleaved," which "ruined" the output image
'''

import sys
from netCDF4 import Dataset
import struct
import os
import time
from hyperspectral_metadata import isDigit

#Note: datatype so far has not included type 6 and 9. They are complex and double-precison complex
#key: ENVI type
#value: (C type, standard size)
DATATYPE = {'1':('H',2),'2':('i',4),'3':('l',4),'4':('f',4),'5':('d',8),'12':('H',4),'13':('L',4),'14':('q',8),'15':('Q',8)}

class TimeMeasurement(object):
	'''
	Supportive class;
	Measuring the time used by unpacking the data
	'''
	def __init__(self,lineName):
		self.lineName = lineName

	def __enter__(self):
		self.startTime = time.time()

	def __exit__(self, *args):
		self.endTime = time.time()
		self.runningTime = (self.endTime - self.startTime) * 1000

		reportHandler = open("PerformanceReport.txt","w")

		prompt = "%s elapsed time: %.3fms, %.5fs"%(self.lineName,
								   				   self.runningTime,
								   				   self.runningTime/1000)
		reportHandler.write(prompt)
		print(prompt)

def getDimension(fileName):
	'''
	Acquire dimensions from related HDR file
	'''
	fileHandler = open(fileName+'.hdr')

	for members in fileHandler.readlines():
		if "samples" in members:
			x = members[members.find("=")+1:len(members)]
		elif "lines" in members:
			y = members[members.find("=")+1:len(members)]
		elif "bands" in members:
			band = members[members.find("=")+1:len(members)]

	return int(band.strip('\n').strip('\r')), int(x.strip('\n').strip('\r')), int(y.strip('\n').strip('\r'))

def getWavelength(fileName):
	'''
	Acquire wavelength(s) from related HDR file
	'''
	fileHandler = open(fileName+'.hdr')
	wavelengthGroup = [float(x.strip('\r').strip('\n').strip(',')) for x in fileHandler.readlines() 
						if isDigit(x.strip('\r').strip('\n').strip(','))]

	return wavelengthGroup

def getHeaderInfo(fileName):
	'''
	Acquire Other Information from related HDR file
	'''
	fileHandler, infoDictionary = open(fileName+'.hdr'), dict()
	for members in fileHandler.readlines():
		if '=' in members and 'wavelength' not in members:
			infoDictionary[members[0:members.find("=")-1]] = members[members.find("=")+2:].strip('\n').strip('\r')

	return infoDictionary

def main(fileName):
	'''
	The main function, reading the data and exporting netCDF file
	'''
	newData = Dataset("WavelengthExp.nc","w",format="NETCDF4")
	dimensionBand, dimensionX, dimensionY = getDimension(fileName)
	wavelength, hdrInfo = getWavelength(fileName), getHeaderInfo(fileName)

	newData.createDimension('band',       dimensionBand)
	newData.createDimension('x',          dimensionX)
	newData.createDimension('y',          dimensionY)
	newData.createDimension('wavelength', len(wavelength))

	mainDataHandler, tempVariable = open('/Users/jeromemao/Desktop/terraref/data'),\
									newData.createVariable('exposure_2','f8',('band', 'x', 'y'))#('band', 'x', 'y')
	fileSize = os.path.getsize(fileName)
	dataNumber, dataType, dataSize = fileSize/DATATYPE[hdrInfo['data type']][-1], DATATYPE[hdrInfo['data type']][0],\
									 DATATYPE[hdrInfo['data type']][-1]

	with TimeMeasurement("unpacking") as lineTiming: #measuring the time
		value = struct.unpack(dataType*dataNumber,mainDataHandler.read(dataSize*dataNumber))#reading the data from the file

	with TimeMeasurement("assigning value") as lineTiming:
		tempVariable[:,:,:] = value #TODO need a better method to assign value to avoid "de-interleaving"

	nestedWavelength    = newData.createVariable('wavelength', 'f8',('wavelength',))
	nestedWavelength[:] = wavelength
	headerInfo          = newData.createGroup("HeaderInfo")

	for members in hdrInfo:
		setattr(headerInfo,members,hdrInfo[members])
		if isDigit(hdrInfo[members]):
			tempVariable = headerInfo.createVariable(members,'i4')
			tempVariable.assignValue(int(hdrInfo[members]))

	newData.close()

if __name__ == '__main__':
	main(sys.argv[1])


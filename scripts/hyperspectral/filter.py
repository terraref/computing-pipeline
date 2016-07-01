'''
This script will automatically check if the file is created after June 01
and will choose the converter accordingly
'''

import os, sys
import time
import EnvironmentalLoggerAnalyser, EnvironmentalLoggerAnalyserUpdated

fileInput, fileOutput = sys.argv[1], sys.argv[2]
fileCreateTime = time.localtime(os.stat(fileInput).st_atime)

if fileCreateTime.tm_mon >= 6:
	EnvironmentalLoggerAnalyserUpdated.mainProgramTrigger(fileInput, fileOutput)
else:
	EnvironmentalLoggerAnalyser.mainProgramTrigger(fileInput, fileOutput)


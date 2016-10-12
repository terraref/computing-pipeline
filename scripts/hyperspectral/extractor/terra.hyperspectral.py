#!/usr/bin/env python

"""
terra.hyperspectral.py

This extractor will trigger when a file is added to a dataset in Clowder.
It checks if all the required input files are present in the dataset while the
output file is not present. The output filename is always determined from the
filename of the `_raw` file.
If the check is OK, it calls the `workerScript` defined in the config file to
create a netCDF output file and adds that to the same dataset.
"""

import os
import subprocess
import logging
from config import *
import pyclowder.extractors as extractors


def main():
	global extractorName, messageType, rabbitmqExchange, rabbitmqURL

	# Set logging
	logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
	logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)

	# Connect to rabbitmq
	extractors.connect_message_bus(
		extractorName        = extractorName,
		messageType          = messageType,
		rabbitmqExchange     = rabbitmqExchange,
		rabbitmqURL          = rabbitmqURL,
		processFileFunction  = process_dataset,
		checkMessageFunction = check_message
	)

def check_message(parameters):
	# Check for expected input files before beginning processing
	if has_all_files(parameters):
		if has_output_file(parameters):
			print 'skipping, output file already exists'
			return False
		else:
			# Handle the message but do not download any files automatically.
			return "bypass"
	else:
		print 'skipping, not all input files are ready'
		return False

# ----------------------------------------------------------------------
# Process the dataset message and upload the results
def process_dataset(parameters):
	global extractorName, workerScript, inputDirectory, outputDirectory

	# Find input files in dataset
	files = get_all_files(parameters)

	# Download files to input directory
	for fileExt in files:
		files[fileExt]['path'] = extractors.download_file(
			channel            = parameters['channel'],
			header             = parameters['header'],
			host               = parameters['host'],
			key                = parameters['secretKey'],
			fileid             = files[fileExt]['id'],
			# What's this argument for?
			intermediatefileid = files[fileExt]['id'],
			ext                = fileExt
		)
		# Restore temp filenames to original - script requires specific name formatting so tmp names aren't suitable
		files[fileExt]['old_path'] = files[fileExt]['path']
		files[fileExt]['path'] = os.path.join(inputDirectory, files[fileExt]['filename'])
		os.rename(files[fileExt]['old_path'], files[fileExt]['path'])
		print 'found %s file: %s' % (fileExt, files[fileExt]['path'])

	# Invoke terraref.sh
	outFilePath = os.path.join(outputDirectory, get_output_filename(files['_raw']['filename']))
	print 'invoking terraref.sh to create: %s' % outFilePath
	returncode = subprocess.call(["bash", workerScript, "-d", "1", "-I", inputDirectory, "-O", outputDirectory])
	print 'done creating output file (%s)' % (returncode)

	if returncode != 0:
		print 'terraref.sh encountered an error'

	# Verify outfile exists and upload to clowder
	if os.path.exists(outFilePath):
		print 'output file detected'
		if returncode == 0:
			print 'uploading output file...'
			extractors.upload_file_to_dataset(filepath=outFilePath, parameters=parameters)
			print 'done uploading'
			print 'extracting metadata in cdl format'
			metaFilePath = outFilePath + '.cdl'
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call('ncks', '--cdl', '-m', '-M', outFilePath], stdout=fmeta)
			if os.path.exists(metaFilePath):
				extractors.upload_file_to_dataset(filepath=metaFilePath, parameters=parameters)

			print 'extracting metadata in xml format'
			metaFilePath = outFilePath + '.xml'
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call('ncks', '--xml', '-m', '-M', outFilePath], stdout=fmeta)
			if os.path.exists(metaFilePath):
				extractors.upload_file_to_dataset(filepath=metaFilePath, parameters=parameters)
		# Clean up the output file.
		os.remove(outFilePath)
	else:
		print 'no output file was produced'

	print 'cleaning up...'
	# Clean up the input files.
	for fileExt in files:
		os.remove(files[fileExt]['path'])
	print 'done cleaning'

# ----------------------------------------------------------------------
# Find as many expected files as possible and return the set.
def get_all_files(parameters):
	global requiredInputFiles
	files = dict()
	for fileExt in requiredInputFiles:
		files[fileExt] = None

	if 'filelist' in parameters:
		for fileItem in parameters['filelist']:
			fileId   = fileItem['id']
			fileName = fileItem['filename']
			for fileExt in files:
				if fileName[-len(fileExt):] == fileExt:
					files[fileExt] = {
						'id': fileId,
						'filename': fileName
					}
	return files

# ----------------------------------------------------------------------
# Returns the output filename.
def get_output_filename(raw_filename):
	return '%s.nc' % raw_filename[:-len('_raw')]

# ----------------------------------------------------------------------
# Returns true if all expected files are found.
def has_all_files(parameters):
	files = get_all_files(parameters)
	allFilesFound = True
	for fileExt in files:
		if files[fileExt] == None:
			allFilesFound = False
	return allFilesFound

# ----------------------------------------------------------------------
# Returns true if the output file is present.
def has_output_file(parameters):
	if 'filelist' not in parameters:
		return False
	if not has_all_files(parameters):
		return False
	files = get_all_files(parameters)
	outFilename = get_output_filename(files['_raw']['filename'])
	outFileFound = False
	for fileItem in parameters['filelist']:
		if outFilename == fileItem['filename']:
			outFileFound = True
			break
	return outFileFound

if __name__ == "__main__":
	main()

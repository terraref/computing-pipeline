'''
This is the unit test module for environmental_logger_json2netcdf.py.
It will test whether the environmental_logger_json2netcdf.py works 
appropriately and the validity of the imported JSON.

This module will run isolated, so there's no include dependency
to other files, but make sure it is in the same location as environmental_logger_json2netcdf

Before running this module, make sure you have passed
it a valid JSON, or all the test will be skipped

To run the unit test, simply use:
python environmental_logger_unittest.py <testing JSON location>
'''

import unittest
import sys
from environmental_logger_json2netcdf import *

fileLocation = sys.argv[1]


class environmental_logger_json2netcdfUnitTest(unittest.TestCase):

	def setUp(self):
		self.testCase = JSONHandler(fileLocation)

	@unittest.skipIf(not os.path.isfile(fileLocation),
					 "the testing JSON file does not exist")
	def test_canGetAWellFormattedJSON(self):
		'''
		This test checks if the EnvironmentalLogger received a legal JSON file. Since
		JSONHandler just simply pass the JSON to built-in JSON module and is guaranteed
		to be noexcept, any error in this test case would be cause by a badly formatted 
		JSON

		Skipped if the file does not exist
		'''

		self.setUp()
		self.assertEqual(len(self.testCase), 4)
		self.assertIs(type(self.testCase[0]), list)

	@unittest.skipIf(not os.path.isfile(fileLocation),
					 "the testing JSON file does not exist")
	def test_canGetExpectedNumberOfWavelength(self):
		'''
		This test checks if the environmental_logger_json2netcdf can get the wavelength by
		testing the number of wvl collected

		Skipped if the file does not exist		
		'''

		self.setUp()
		self.assertEqual(len(self.testCase[1]), 1024)
		self.assertIsInstance(self.testCase[1][0], float)


	@unittest.skipIf(not os.path.isfile(fileLocation),
					 "the testing JSON file does not exist")
	def test_canGetExpectedNumberOfSpectrum(self):
		'''
		This test checks if the environmental_logger_json2netcdf can get the spectrum by
		testing whether it is a 2D-array (It is implemented as a 2D-array)

		Skipped if the file does not exist		
		'''

		self.setUp()
		self.assertEqual(len(self.testCase), 4)
		self.assertEqual(len(self.testCase[2]), 39)

	@unittest.skipIf(not os.path.isfile(fileLocation),
					 "the testing JSON file does not exist")
	def test_canGetAListOfValueFromImportedJSON(self):
		'''
		This test checks if the environmental_logger_json2netcdf can get any value from 
		the JSON (as a list)

		Skipped if the file does not exist		
		'''

		self.setUp()
		testingJSON = [JSONMembers[u"environment_sensor_set_reading"] for JSONMembers in self.testCase[0]]
		self.assertIsInstance(getListOfValue(testingJSON, u"weatherStationAirPressure"), list)
		self.assertIsInstance(getListOfValue(testingJSON, u"weatherStationAirPressure")[0], float)
		self.assertEqual(len(getListOfValue(testingJSON, u"weatherStationAirPressure")), 39)

	@unittest.skipIf(not os.path.isfile(fileLocation),
					 "the testing JSON file does not exist")
	def test_canGetAListOfRawValueFromImportedJSON(self):
		'''
		This test checks if the environmental_logger_json2netcdf can get any raw value from 
		the JSON (as a list)

		Skipped if the file does not exist		
		'''

		self.setUp()
		testingJSON = [JSONMembers[u"environment_sensor_set_reading"] for JSONMembers in self.testCase[0]]
		self.assertIsInstance(getListOfRawValue(testingJSON, u"weatherStationAirPressure"), list)
		self.assertIsInstance(getListOfRawValue(testingJSON, u"weatherStationAirPressure")[0], float)
		self.assertEqual(len(getListOfRawValue(testingJSON, u"weatherStationAirPressure")), 39)


	def test_canTranslateIntoLegalName(self):
		pass
		

	def tearDown(self):
		pass


if __name__ == "__main__":
	unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromTestCase(environmental_logger_json2netcdfUnitTest))

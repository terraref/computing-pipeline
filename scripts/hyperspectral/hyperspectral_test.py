import unittest
import sys
from netCDF4   import Dataset
from functools import wraps

'''
Test for Hyperspectral Workflow

This script does not check whether all the data are correct (because there are too many of them),
instead, it will check whether it has enough number of groups, dimensions, and variables,
and will take one or two samples to check the values.

==============================================================================
To run the test from the commandline, do:
python hyperspectral_test.py <the input netCDF file>

==============================================================================
It will check the followings so far:
1. Have enough number of root level groups
2. Have enough number of root level dimensions
3. Have enough number of root level variables
4. The dimensions are all correct (in both name and numerical value)
5. The groups are all correctly named
6. The wavelengths are correctly written
7. The georeferencing data are correctly recorded
8. The RGB indices are correctly recorded (in both name and numerical value)
9. The history is correctly recorded (match the regex pattern)

'''


EXPECTED_NUMBER_OF_GROUPS     = 6
EXPECTED_NUMBER_OF_DIMENSIONS = 4
TEST_FILE_DIRECTORY           = None

class HyperspectralWorkFlowTestWidget:

    @staticmethod
    def skipIfDontHaveEnoughGroups(func):
        def innerWrapper(self):
            if len(self.groups) == 6:
                return unittest.skip("Skipped because failed to satisfy the number requirements")
        return innerWrapper


class HyperspectralWorkFlowTest(unittest.TestCase, HyperspectralWorkFlowTestWidget):

    @classmethod
    def setUpClass(cls):
        '''
        Set up the environment before all the test cases are triggered
        '''
        cls.masterNetCDFHandler = Dataset(TEST_FILE_DIRECTORY, "r")
        cls.groups     = cls.masterNetCDFHandler.groups
        cls.dimensions = cls.masterNetCDFHandler.dimensions

    @classmethod
    def tearDownClass(cls):
        '''
        Do the clean up after all the test cases were finished
        '''
        cls.masterNetCDFHandler.close()

    #################### Test Cases ####################

    def testTheNumberOfGroupsInRootLevelIsCorrect(self):
        '''
        Check if there are six groups in the root level
        '''
        self.assertEqual(len(self.groups), EXPECTED_NUMBER_OF_GROUPS, msg="There should be six groups total")

    @unittest.expectedFailure
    def testTheNumberOfDimensionsInRootLevelIsCorrect(self):
        '''
        Check if there are four dimensions in the root level
        '''
        self.assertEqual(len(self.dimensions), EXPECTED_NUMBER_OF_DIMENSIONS, msg="There should be four dimensions total")

    def testTheTimeDimensionsHaveCorrectValues(self):
        '''
        Check if all the dimensions have the right values
        '''
        self.assertEqual(len(self.dimensions["time"]), 169,  msg="The dimension for time should be 169")

    def testTheXDimensionsHaveCorrectValues(self):
        self.assertEqual(len(self.dimensions["x"]),    1600, msg="The dimension for x should be 1600")

    def testTheYDimensionsHaveCorrectValues(self):
        self.assertEqual(len(self.dimensions["y"]),    169,  msg="The dimension for y should be 1600")

    def testTheWavelengthDimensionsHaveCorrectValues(self):
        self.assertIn(len(self.dimensions["wavelength"]), (272, 955), msg="The dimension for wavelength should be either 272 or 955")

    @HyperspectralWorkFlowTestWidget.skipIfDontHaveEnoughGroups
    def testTheGantrySystemFixedMetadataGroupIsCorrectlyNamed(self):
        '''
        Check if all the groups are named as what we want
        '''
        self.assertIn("gantry_system_fixed_metadata", self.groups, msg="gantry_system_fixed_metadata should be a group in root level")
        
    def testTheSensorFixedMetadataGroupIsCorrectlyNamed(self):
        self.assertIn("sensor_fixed_metadata", self.groups, msg="sensor_fixed_metadata should be a group in root level")
        
    def testTheGantrySystemVariableMetadataGroupIsCorrectlyNamed(self):
        self.assertIn("gantry_system_variable_metadata", self.groups, msg="gantry_system_variable_metadata should be a group in root level")
        
    def testTheUserGivenMetadataGroupIsCorrectlyNamed(self):
        self.assertIn("user_given_metadata", self.groups, msg="user_given_metadata should be a group in root level")
        
    def testTheSensorVariableMetadataGroupIsCorrectlyNamed(self):
        self.assertIn("sensor_variable_metadata", self.groups, msg="gantry_system_fixed_metadata should be a group in root level")
        
    def testTheHeaderInfoGroupIsCorrectlyNamed(self):
        self.assertIn("header_info", self.groups, msg="header_info should be a group in root level")

    def testWavelengthArrayHasEnoughData(self):
        '''
        Roughly check if there are enough numbers of wavelengths and compare their values
        '''
        self.wavelengthArray = self.masterNetCDFHandler.variables['wavelength']
        self.assertIn(len(self.wavelengthArray), (272, 955), msg="The length of the wavelength must be in 272 or 955")
    
    def testWavelengthArrayHasCorrectData(self):
        self.wavelengthArray = self.masterNetCDFHandler.variables['wavelength']

        self.assertGreater(self.wavelengthArray[0], 300,     msg="The first sample of the wavelength should greater than 300nm")
        self.assertLess(   self.wavelengthArray[0], 1000,    msg="The last sample of the wavelength should greater than 1000nm")

    def testHistoryIsCorrectlyRecorded(self):
        '''
        Check if the product has a correct attribute called "history"
        '''
        self.assertTrue(getattr(self.masterNetCDFHandler, "history"), msg="The product must have an attribute called history")
        
        self.historyData = self.masterNetCDFHandler.history
        self.assertRegexpMatches(self.historyData,
                                 r'[a-zA-Z]{3}\s[a-zA-Z]{3}\s[\d]{1,2}\s[\d]{2}[:][\d]{2}[:][\d]{2}\s[\d]{4}[:]\spython\s.*', 
                                 msg="The history string should anyhow larger than 0")
    
    def testFrameTimeHasCorrectCalendarAttr(self):
        self.assertIn("frametime", self.masterNetCDFHandler.variables, msg="The calender should be in the root level")

        self.frameTime = self.masterNetCDFHandler.variables["frametime"]
        self.assertEqual(self.frameTime.calender, "gregorian", msg="The calender for frametime is gregorian")

    def testFrameTimeHasCorrectUnitsAttr(self): 
        self.frameTime = self.masterNetCDFHandler.variables["frametime"]       
        self.assertEqual(self.frameTime.units, "days since 1970-01-01 00:00:00", msg="The units for frametime should be based on Unix-basetime")

    def testFrameTimeHasCorrectValue(self): 
        self.frameTime = self.masterNetCDFHandler.variables["frametime"]       
        self.assertGreater(self.frameTime[0], 17000, msg="The value for frametime should anyhow larger than 17000")
    
    def testRedBandIndexIsCorrectlyRecorded(self):
        '''
        Check if there are three band indices and their values are correct
        '''
        self.headerInformation = self.groups["header_info"]
        self.redIndex   = self.headerInformation.variables["red_band_index"]

        self.assertEqual(self.redIndex[...],   235, msg="The value of red_band_index is always 235")

    def testBlueBandIndexIsCorrectlyRecorded(self):
        self.headerInformation = self.groups["header_info"]

        self.blueIndex  = self.headerInformation.variables["blue_band_index"]
        self.assertEqual(self.blueIndex[...],  141, msg="The value of blue_band_index is always 141")

    def testGreenBandIndexIsCorrectlyRecorded(self):
        self.headerInformation = self.groups["header_info"]

        self.greenIndex = self.headerInformation.variables["green_band_index"]
        self.assertEqual(self.greenIndex[...], 501, msg="The value of green_band_index is always 501")

    def testGreenBandIndexIsUnsignedShortInteger(self):
        self.headerInformation = self.groups["header_info"]

        self.greenIndex = self.headerInformation.variables["green_band_index"]
        self.assertEqual(self.greenIndex.dtype, "u2", msg="Indices must be saved as unsigned short integers")

    def testBlueBandIndexIsUnsignedShortInteger(self):
        self.headerInformation = self.groups["header_info"]

        self.blueIndex = self.headerInformation.variables["blue_band_index"]
        self.assertEqual(self.blueIndex.dtype, "u2", msg="Indices must be saved as unsigned short integers")

    def testRedBandIndexIsUnsignedShortInteger(self):
        self.headerInformation = self.groups["header_info"]

        self.redIndex = self.headerInformation.variables["red_band_index"]
        self.assertEqual(self.redIndex.dtype, "u2", msg="Indices must be saved as unsigned short integers")

    def testXHaveCorrectValuesAndAttributes(self):
        '''
        Check if the georeferencing data are correct (for x and y)
        '''
        self.x = self.masterNetCDFHandler.variables["x"]
        self.assertEqual(len(self.x), 1600, msg="The width of the image should always be 1600 pxl")
        self.assertEqual(self.x.units, "meter", msg="The unit for x should always be meter")

    def testYHaveCorrectValuesAndAttributes(self):
        '''
        Check if the georeferencing data are correct (for x and y)
        '''
        self.y = self.masterNetCDFHandler.variables["y"]
        self.assertEqual(len(self.y), 169,  msg="The height of the image should always be 169 pxl")
        self.assertEqual(self.y.units, "meter", msg="The unit for y should always be meter")

    def testPositionVariablesAreCorrectlyFormatted(self):
        self.variable_metadata = self.groups["gantry_system_variable_metadata"].variables
        self.assertIn("position_x", self.variable_metadata, msg="The position should be named as position x")

        self.assertEqual(self.variable_metadata["position_x"].units, "meter", msg="The position should has an unit of meter")
        self.assertEqual(self.variable_metadata["position_x"].long_name, "Position in X Direction", msg="The position should has a correctly formatted long name")

    def testSpeedVariablesAreCorrectlyFormatted(self):
        self.variable_metadata = self.groups["gantry_system_variable_metadata"].variables
        self.assertIn("speed_x", self.variable_metadata, msg="The position should be named as speed x")

        self.assertEqual(self.variable_metadata["speed_x"].units, "meter second-1", msg="The speed should has an unit of meter second-1")
        self.assertEqual(self.variable_metadata["speed_x"].long_name, "Gantry Speed in X Direction", msg="The speed should has a correctly formatted long name")


if __name__ == "__main__":
    TEST_FILE_DIRECTORY = sys.argv[1]
    testSuite = unittest.TestLoader().loadTestsFromTestCase(HyperspectralWorkFlowTest)
    unittest.TextTestRunner(verbosity=3).run(testSuite)
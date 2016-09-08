import numpy as np
import sys
import json

__all__ = [pixel2Geographic,]

# from Dr. LeBauer, Github thread: terraref/referece-data #32
CAMERA_POSITION = np.array([1.9, 0.855, 0.635])

# from Dr. LeBauer, Github thread: terraref/referece-data #32
CAMERA_FOCAL_LENGTH = 24e-3 # the focal length for SWIR camera. unit:[m]

# from Dr. LeBauer, Github thread: terraref/referece-data #32
PIXEL_PITCH = 25e-6 #[m]

# from Dr. LeBauer, Github thread: terraref/referece-data #32
# Originally in 33, 04.470' N / -111, 58.485' W
REFERENCE_POINT_LATLONG = np.deg2rad(33 + 4.470 / 60), np.deg2rad(-111 - 58.485 / 60) +np.pi # Temporarily
#print REFERENCE_POINT_LATLONG

# from Dr. LeBauer, Github thread: terraref/referece-data #32
GAMMA = 0 #TODO: waiting for the correct value

# from Dr. LeBauer, Github thread: terraref/referece-data #32
# This matrix looks like this:
#
#     | alphaX, gamma, u0 |
#     |			  |
# A = |   0 ,  alphaY, v0 |
#     |			  |
#     |   0 ,    0,     1 |
#
# where alphaX = alphaY = CAMERA_FOCAL_LENGTH / PIXEL_PITCH,
#       GAMMA is calibration constant
#       u0 and v0 are the center coordinate of the image (waiting to be found)
#
# will be used in calculating the lat long of the image

ORIENTATION_MATRIX = np.array([[CAMERA_FOCAL_LENGTH / PIXEL_PITCH, GAMMA, 0], [0, CAMERA_FOCAL_LENGTH / PIXEL_PITCH, 0 ], [0, 0, 1]])

def pixel2Geographic(jsonFileLocation, headerFileLocation):

    ######################### Load necessary data #########################
    with open(jsonFileLocation) as fileHandler:
        master = json.loads(fileHandler.read())["lemnatec_measurement_metadata"]
        
        x_gantry_pos = float(master["gantry_system_variable_metadata"]["position x [m]"])
        y_gantry_pos = float(master["gantry_system_variable_metadata"]["position y [m]"])

        x_camera_pos = 1.9 # From https://github.com/terraref/reference-data/issues/32
        y_camera_pos = 0.855

        x_pixel_size = y_pixel_size = 0.98526434004512529576754637665e-3

        x_pixel_num, y_pixel_num = 0, 0 #placeholder for x and y pixel numbers

        with open(headerFileLocation) as fileHandler:
            overall = fileHandler.readlines()

            for members in overall:
                if "width" in members:
                    x_pixel_num = int(members.split("=")[-1].strip("\n"))
                elif "height" in members:
                    y_pixel_num = int(members.split("=")[-1].strip("\n"))


        ######################### Do calculation #########################

        x_absolute_pos = x_gantry_pos + x_camera_pos
        y_absolute_pos = y_gantry_pos + y_camera_pos

        x_final_result = numpy.array([x * x_pixel_size for x in range(x_pixel_num)]) + x_absolute_pos
        y_final_result = numpy.array([y * y_pixel_size for y in range(y_pixel_num)]) + y_absolute_pos

        ########### Sample result: x -> 0.377 [m], y -> 0.267 [m] ###########

        bounding_box = x_final_result[-1] - x_final_result[0], y_final_result[-1] - y_final_result[0] 

        return x_final_result, y_final_result, bounding_box
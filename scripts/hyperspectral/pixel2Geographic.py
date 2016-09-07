import sys
import json
import numpy

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


if __name__ == "__main__":
    pixel2Geographic(sys.argv[1], sys.argv[2])
import os
import sys
from datetime import date, timedelta

stereotop_dir = '/data/terraref/sites/ua-mac/raw_data/stereoTop/'
rgb_geotiff_dir = '/data/terraref/sites/ua-mac/Level_1/rgb_geotiff/'
flir_ir_dir = '/data/terraref/sites/ua-mac/raw_data/flirIrCamera/'
ir_geotiff_dir = '/terraref/sites/ua-mac/Level_1/ir_geotiff/'


def get_counts_for_date(date_string):
    result = date_string

    current_stereotop_dir = stereotop_dir+date_string
    current_rgb_geotiff_dir = rgb_geotiff_dir + date_string
    current_flir_ir_dir = flir_ir_dir + date_string
    current_ir_geotiff_dir = ir_geotiff_dir + date_string

    stereotop_count = 0
    rgb_geotiff_count = 0
    flir_ir_count = 0
    ir_geotiff_count = 0

    if os.path.exists(current_stereotop_dir):
        stereotop_count = len(os.listdir(current_stereotop_dir))
    else:
        print(current_stereotop_dir + ' does not exist')
    result += ','
    result += str(stereotop_count)

    if os.path.exists(current_rgb_geotiff_dir):
        rgb_geotiff_count = len(os.listdir(current_rgb_geotiff_dir))
    else:
        print(current_ir_geotiff_dir + ' does not exist')
    result += ','
    result += str(rgb_geotiff_count)

    if os.path.exists(current_flir_ir_dir):
        flir_ir_count = len(os.listdir(current_flir_ir_dir))
    else:
        print(current_flir_ir_dir + 'does not exist')
    result += ','
    result += str(flir_ir_count)

    if os.path.exists(current_rgb_geotiff_dir):
        ir_geotiff_count = len(os.listdir(current_ir_geotiff_dir))
    else:
        print(current_rgb_geotiff_dir + ' does not exist')
    result += ','
    result += str(ir_geotiff_count)
    return result


def main():
    all_counts = []
    command_line_arguments = sys.argv[1:]
    #print(command_line_arguments)
    if len(command_line_arguments) == 1:
        date_string = sys.argv[1]
        date_string = date_string.split('-')
        current_date = date(int(date_string[0]), int(date_string[1]), int(date_string[2]))
        current_result = get_counts_for_date(str(current_date))
        all_counts.append(current_result)
        print(current_date)
    else:
        start_date_string = sys.argv[1]
        start_date_string = start_date_string.split('-')
        start_date = date(int(start_date_string[0]), int(start_date_string[1]), int(start_date_string[2]))
        #print(start_date)
        end_date_string = sys.argv[2]
        end_date_string = end_date_string.split('-')
        end_date = date(int(end_date_string[0]), int(end_date_string[1]), int(end_date_string[2]))
        delta = end_date - start_date
        for i in range(delta.days + 1):
            current_result = get_counts_for_date(str(start_date + timedelta(i)))
            all_counts.append(current_result)
            #print(start_date + timedelta(i))

    for i in range(0, len(all_counts)):
        print(all_counts[i])

if __name__ == '__main__':
    main()
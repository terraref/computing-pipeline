import os
import pandas as pd
import datetime
import psycopg2
from .counter_config import dbname, user, host, password

stereotop_dir = '/data/terraref/sites/ua-mac/raw_data/stereoTop/'
rgb_geotiff_dir = '/data/terraref/sites/ua-mac/Level_1/rgb_geotiff/'
flir_ir_dir = '/data/terraref/sites/ua-mac/raw_data/flirIrCamera/'
ir_geotiff_dir = '/terraref/sites/ua-mac/Level_1/ir_geotiff/'


# TODO make these refer to os environ

rgb_geotiff_csv = '2018 Pipeline Status - rgb_geotiff.csv'
ir_geotiff_csv = '2018 Pipeline Status - ir_geotiff.csv'
check_table_csv = '2018 Pipeline Status - CHECK_TABLE.csv'

PERCENT_COMPLETE_MINIMUM = 0.99

MINIMUM_DATE_STRING = '2018-01-01'


def generate_dates_in_range(start_date_string):
    start_date = datetime.datetime.strptime(start_date_string, '%Y-%m-%d')
    todays_date = datetime.datetime.now()
    days_between = (todays_date - start_date).days
    date_strings = []
    for i in range(0, days_between+1):
        current_date = start_date + datetime.timedelta(days=i)
        current_date_string = current_date.strftime('%Y-%m-%d')
        date_strings.append(current_date_string)
    return date_strings

def update_csv_file_rulemonitor_rgb(path_to_file, dates_to_check):
    df = pd.read_csv(path_to_file)
    # all_dates = df['Date'].tolist()

    indices = list(df.column.values)

    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password)
    for current_date in dates_to_check:
        if (df['Date'] == current_date).any():
            output_string = 'Full Field -- RGB GeoTIFFs - ' + current_date + '%'
            query = "select count(distinct file_path) from extractor_ids where output like '%s';"
            query = query % output_string
            curs = conn.cursor()
            curs.execute(query)
            results = []
            for result in curs:
                results.append(result)
            value = int(results[0][0])
            df.loc[df['Date'] == current_date, 'rgb ruleDB'] = value
    df.to_csv(path_to_file, index=False)

def update_csv_file_rulemonitor_ir_ruledb(path_to_file, dates_to_check):
    df = pd.read_csv(path_to_file)
    # all_dates = df['Date'].tolist()

    indices = list(df.column.values)

    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password)
    for current_date in dates_to_check:
        if (df['Date'] == current_date).any():
            # TODO change name
            output_string = 'Full Field -- RGB GeoTIFFs - ' + current_date + '%'
            query = "select count(distinct file_path) from extractor_ids where output like '%s';"
            query = query % output_string
            curs = conn.cursor()
            curs.execute(query)
            results = []
            for result in curs:
                results.append(result)
            value = int(results[0][0])
            df.loc[df['Date'] == current_date, 'rgb ruleDB'] = value
    df.to_csv(path_to_file, index=False)

def update_csv_file_rgb_geotiff(path_to_file, dates_to_check):

    df = pd.read_csv(path_to_file)
    # all_dates = df['Date'].tolist()

    indices = list(df.column.values)

    for current_date in dates_to_check:
        if (df['Date'] == current_date).any():
            percent_complete = float(df.loc[df['Date'] == current_date, 'bin2tif%'].values[0])
            if percent_complete < PERCENT_COMPLETE_MINIMUM:
                num_stereotop_files = len(os.listdir(stereotop_dir + current_date))
                num_rgb_geotiff_files = len(os.listdir(rgb_geotiff_dir + current_date))
                new_percent_complete = float(num_rgb_geotiff_files/num_stereotop_files)
                if new_percent_complete > percent_complete:
                    df.loc[df['Date'] == current_date, 'stereoTop'] = num_stereotop_files
                    df.loc[df['Date'] == current_date, 'rgb_geotiff'] = num_rgb_geotiff_files
                    df.loc[df['Date'] == current_date, 'bin2tif%'] = new_percent_complete
        else:
            num_stereotop_files = len(os.listdir(stereotop_dir + current_date))
            num_rgb_geotiff_files = len(os.listdir(rgb_geotiff_dir + current_date))
            new_percent_complete = float(num_rgb_geotiff_files / num_stereotop_files)
            new_entry = [current_date, num_stereotop_files, num_rgb_geotiff_files, new_percent_complete
                         , '', '', '', '', '', '', '', '', '']
            df = df.append(pd.Series(new_entry, index=indices), ignore_index=True)

    df.to_csv(path_to_file, index=False)


def update_csv_file_ir_geotiff(path_to_file, dates_to_check):
    df = pd.read_csv(path_to_file)
    ## all_dates = df['Date'].tolist()

    indices = list(df.column.values)


    for current_date in dates_to_check:
        if (df['Date'] == current_date).any():
            percent_complete = float(df.loc[df['Date'] == current_date, 'flir2tif%'].values[0])
            if percent_complete < PERCENT_COMPLETE_MINIMUM:
                num_ir_files = len(os.listdir(ir_geotiff_dir + current_date))
                num_ir_geotiff_files = len(os.listdir(ir_geotiff_dir + current_date))
                new_percent_complete = float(num_ir_files/num_ir_geotiff_files)
                if new_percent_complete > percent_complete:
                    df.loc[df['Date'] == current_date, 'flirIrCamera'] = num_ir_files
                    df.loc[df['Date'] == current_date, 'ir_geotiff'] = num_ir_geotiff_files
                    df.loc[df['Date'] == current_date, 'flir2tif%'] = new_percent_complete
        else:
            num_ir_files = len(os.listdir(ir_geotiff_dir + current_date))
            num_ir_geotiff_files = len(os.listdir(ir_geotiff_dir + current_date))
            new_percent_complete = float(num_ir_files / num_ir_geotiff_files)
            new_entry = [current_date, num_ir_files, num_ir_geotiff_files, new_percent_complete
                , '', '', '', '', '', '', '', '', '']
            df = df.append(pd.Series(new_entry, index=indices), ignore_index=True)
    df.to_csv(path_to_file, index=False)


def main():
    dates_in_range = generate_dates_in_range(MINIMUM_DATE_STRING)

    update_csv_file_rgb_geotiff(check_table_csv, dates_in_range)
    update_csv_file_ir_geotiff(check_table_csv, dates_in_range)
    update_csv_file_rulemonitor_rgb(check_table_csv, dates_in_range)

if __name__ == '__main__':
    main()
import os
import pandas as pd
import datetime
import psycopg2
import re
from collections import OrderedDict


"""
Dictionary of count definitions for various sensors.

Types:
    timestamp:  count timestamp directories in each date directory
    psql:       count rows returned from specified postgres query
    regex:      count files within each date directory that match regex
Other fields:
    path:       path containing date directories for timestamp or regex counts
    regex:      regular expression to execute on date directory for regex counts
    query:      postgres query to execute for psql counts
    parent:     previous count definition for % generation (e.g. bin2tif's parent is stereoTop)
"""
sites_root = "/home/clowder/"
count_defs = {
    "stereoTop": OrderedDict([
        ("stereoTop", {
            "path": os.path.join(sites_root, 'sites/ua-mac/raw_data/stereoTop/'),
            "type": 'timestamp'}),
        ("bin2tif", {
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_1/rgb_geotiff/'),
            "type": 'timestamp',
            "parent": "stereoTop"}),
        ("rulechecker", {
            "type": "psql",
            "query": "select count(distinct file_path) from extractor_ids where output like 'Full Field -- RGB GeoTIFFs - %s%%';",
            "parent": "bin2tif"}),
        ("fullfield", {
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_1/fullfield/'),
            "type": 'regex',
            "regex": "^.*\d+_rgb_.*thumb.tif"}),
        ("canopycover", {
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_1/fullfield/'),
            "type": 'regex',
            "regex": '',
            "parent": "fullfield"})
    ]),
    "flirIrCamera": OrderedDict([
        ("flirIrCamera", {
            "path": os.path.join(sites_root, 'sites/ua-mac/raw_data/flirIrCamera/'),
            "type": 'timestamp'}),
        ("flir2tif", {
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_1/ir_geotiff/'),
            "type": 'timestamp',
            "parent": "flirIrCamera"}),
        ("rulechecker", {
            "type": "psql",
            "query": "select count(distinct file_path) from extractor_ids where output like 'Full Field -- Thermal IR GeoTIFFs - %s%%';",
            "parent": "flir2tif"}),
        ("fullfield", {
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_1/fullfield/'),
            "type": 'regex',
            "regex": "^.*\d+_ir_.*thumb.tif"}),
        ("meantemp", {
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_1/fullfield/'),
            "type": 'regex',
            "regex": '',
            "parent": "fullfield"})
    ])
}

MIN_PERCENT = 1
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

def perform_count(target_def, date, conn):
    """Return count of specified type"""

    count = 0
    if target_def["type"] == "timestamp":
        date_dir = os.path.join(target_def["path"], date)
        if os.path.exists(date_dir):
            count = len(os.listdir(date_dir))

    elif target_def["type"] == "regex":
        date_dir = os.path.join(target_def["path"], date)
        if os.path.exists(date_dir):
            for timestamp in os.listdir(date_dir):
                ts_dir = os.path.join(date_dir, timestamp)
                for file in os.listdir(ts_dir):
                    if re.match(target_def["regex"], file):
                        count += 1

    elif target_def["type"] == "psql":
        query_string = target_def["query"] % date
        curs = conn.cursor()
        curs.execute(query_string)
        for result in curs:
            count = result[0]

    return count

def update_file_counts(sensor, dates_to_check, conn):
    """Perform necessary counting to update CSV."""

    output_file = sensor+"_PipelineWatch.csv"
    print("Updating counts for %s into %s" % (sensor, output_file))
    targets = count_defs[sensor]

    # Load data frame from existing CSV or create a new one
    if os.path.exists(output_file):
        df = pd.read_csv(output_file)
    else:
        cols = ["date"]
        for target_count in targets:
            target_def = targets[target_count]

            cols.append(target_count)
            if "parent" in target_def:
                cols.append(target_count+'%')

        df = pd.DataFrame(columns=cols)

    for current_date in dates_to_check:
        print("...scanning %s" % current_date)
        counts = {}
        percentages = {}

        # Populate count and percentage (if applicable) for each target count
        for target_count in targets:
            target_def = targets[target_count]
            counts[target_count] = perform_count(target_def, current_date, conn)
            if "parent" in target_def:
                if target_def["parent"] not in counts:
                    counts[target_def["parent"]] = perform_count(targets[target_def["parent"]], current_date, conn)
                if counts[target_def["parent"]] > 0:
                    percentages[target_count] = float(counts[target_count]/counts[target_def["parent"]])
                else:
                    percentages[target_count] = 0
        # If this date already has a row, just update
        if (df['Date'] == current_date).any():
            for target_count in targets:
                target_def = targets[target_count]
                df.loc[df['date'] == current_date, target_count] = counts[target_count]
                if "parent" in target_def:
                    df.loc[df['date'] == current_date, target_count+'%'] = percentages[target_count+'%']

        # If not, create a new row
        else:
            new_entry = [current_date]
            indices = ["date"]

            for target_count in targets:
                target_def = targets[target_count]

                indices.append(target_count)
                new_entry.append(counts[target_count])
                if "parent" in target_def:
                    indices.append(target_count+'%')
                    new_entry.append(percentages[target_count])

            df = df.append(pd.Series(new_entry, index=indices), ignore_index=True)

    df.to_csv(output_file, index=False)


def main():
    conn = psycopg2.connect(dbname="rulemonitor", user="rulemonitor", host="192.168.5.169", password=SECRET)
    dates_in_range = generate_dates_in_range(MINIMUM_DATE_STRING)

    for sensor in count_defs:
        update_file_counts(sensor, dates_in_range, conn)

if __name__ == '__main__':
    main()
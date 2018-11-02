import os, thread, json, collections
import logging, logging.config, logstash
import time
import pandas as pd
import datetime
import psycopg2
import re
from collections import OrderedDict
from flask import Flask, render_template, send_file, request, url_for, redirect, make_response


config = {}

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
app_dir = "/home/filecounter/"
sites_root = "/home/clowder/"
SENSOR_COUNT_DEFINITIONS = {
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
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_2/rgb_canopycover/'),
            "type": 'regex',
            "regex": '.*_canopycover_bety.csv',
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
            "path": os.path.join(sites_root, 'sites/ua-mac/Level_2/ir_meantemp/'),
            "type": 'regex',
            "regex": '.*_meantemp_bety.csv',
            "parent": "fullfield"})
    ])
}

SCAN_LOCK = False

"""Load contents of .json file into a JSON object"""
def loadJsonFile(filename):
    try:
        f = open(filename)
        jsonObj = json.load(f)
        f.close()
        return jsonObj
    except IOError:
        logger.error("- unable to open %s" % filename)
        return {}

"""Nested update of python dictionaries for config parsing"""
def updateNestedDict(existing, new):
    # Adapted from http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    for k, v in new.iteritems():
        if isinstance(existing, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = updateNestedDict(existing.get(k, {}), v)
                existing[k] = r
            else:
                existing[k] = new[k]
        else:
            existing = {k: new[k]}
    return existing

def create_app(test_config=None):

    pipeline_csv = os.path.join(config['csv_path'], "{}.csv")

    sensor_names = get_sensor_names()

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/sensors')
    def sensors():
        return render_template('sensors.html', sensors=sensor_names)

    @app.route('/download/<sensor_name>')
    def download(sensor_name):
        current_csv = pipeline_csv.format(sensor_name)
        current_csv_name = os.path.basename(current_csv)
        return send_file(current_csv,
                         mimetype='text/csv',
                         attachment_filename=current_csv_name,
                         as_attachment=True)

    @app.route('/showcsv/<sensor_name>', defaults={'days': 14})
    @app.route('/showcsv/<sensor_name>/<int:days>')
    def showcsv(sensor_name, days):
        # data = dataset.html
        current_csv = pipeline_csv.format(sensor_name)
        df =  pd.read_csv(current_csv, index_col=False)
        if days == 0:
            return df.to_html()
        else:
            return df.tail(days).to_html()

    @app.route('/schedule/<sensor_name>/<start_range>', defaults={'end_range': None})
    @app.route('/schedule/<sensor_name>/<start_range>/<end_range>')
    def schedule_count(sensor_name, start_range, end_range):
        if sensor_name.lower() == "all":
            sensors = get_sensor_names()
        else:
            sensors = [sensor_name]

        dates_in_range = generate_dates_in_range(start_range, end_range)

        psql_db = os.getenv("RULECHECKER_DATABASE", config['postgres']['database'])
        psql_host = os.getenv("RULECHECKER_HOST", config['postgres']['host'])
        psql_user = os.getenv("RULECHECKER_USER", config['postgres']['username'])
        psql_pass = os.getenv("RULECHECKER_PASSWORD", config['postgres']['password'])

        conn = psycopg2.connect(dbname=psql_db, user=psql_user, host=psql_host, password=psql_pass)

        thread.start_new_thread(update_file_counts, (sensors, dates_in_range, conn))

        return "Custom scan scheduled for %s sensors and %s dates" % (len(sensors), len(dates_in_range))

    return app

def get_sensor_names():
    return SENSOR_COUNT_DEFINITIONS.keys()

def generate_dates_in_range(start_date_string, end_date_string=None):
    start_date = datetime.datetime.strptime(start_date_string, '%Y-%m-%d')
    if not end_date_string:
        end_date = datetime.datetime.now()
    else:
        end_date = datetime.datetime.strptime(end_date_string, '%Y-%m-%d')
    days_between = (end_date - start_date).days

    date_strings = []
    for i in range(0, days_between+1):
        current_date = start_date + datetime.timedelta(days=i)
        current_date_string = current_date.strftime('%Y-%m-%d')
        date_strings.append(current_date_string)
    return date_strings

def perform_count(target_count, target_def, date, conn):
    """Return count of specified type"""

    count = 0
    if target_def["type"] == "timestamp":
        date_dir = os.path.join(target_def["path"], date)
        if os.path.exists(date_dir):
            logging.info("   [%s] counting timestamps in %s" % (target_count, date_dir))
            count = len(os.listdir(date_dir))

    elif target_def["type"] == "regex":
        date_dir = os.path.join(target_def["path"], date)
        if os.path.exists(date_dir):
            logging.info("   [%s] matching regex against %s" % (target_count, date_dir))
            for date_content in os.listdir(date_dir):
                if os.path.isdir(date_content):
                    # This is timestamp-level search
                    ts_dir = os.path.join(date_dir, date_content)
                    for file in os.listdir(ts_dir):
                        if re.match(target_def["regex"], file):
                            count += 1
                else:
                    # No timestamp (e.g. fullfield)
                    if re.match(target_def["regex"], date_content):
                        count += 1

    elif target_def["type"] == "psql":
        logging.info("   [%s] querying PSQL records for %s" % (target_count, date))
        query_string = target_def["query"] % date
        curs = conn.cursor()
        curs.execute(query_string)
        for result in curs:
            count = result[0]

    return count

def run_update():
    psql_db = os.getenv("RULECHECKER_DATABASE", config['postgres']['database'])
    psql_host = os.getenv("RULECHECKER_HOST", config['postgres']['host'])
    psql_user = os.getenv("RULECHECKER_USER", config['postgres']['username'])
    psql_pass = os.getenv("RULECHECKER_PASSWORD", config['postgres']['password'])

    conn = psycopg2.connect(dbname=psql_db, user=psql_user, host=psql_host, password=psql_pass)

    while True:
        # Determine two weeks before current date by default
        today = datetime.datetime.now()
        two_weeks = today - datetime.timedelta(days=14)
        start_date_string = os.getenv('START_SCAN_DATE', two_weeks.strftime("%Y-%m-%d"))
        dates_to_check = generate_dates_in_range(start_date_string)

        logging.info("Checking counts for dates %s - %s" % (start_date_string, dates_to_check[-1]))
        update_file_counts(get_sensor_names(), dates_to_check, conn)

        # Wait 1 hour for next iteration
        time.sleep(3600)

def update_file_counts(sensors, dates_to_check, conn):
    """Perform necessary counting to update CSV."""
    global SCAN_LOCK

    while SCAN_LOCK:
        logging.info("Another thread currently locking database; waiting 60 seconds to retry")
        time.sleep(60)

    logging.info("Locking scan for %s sensors and %s dates" % (len(sensors), len(dates_to_check)))
    SCAN_LOCK = True
    for sensor in sensors:
        output_file = os.path.join(config['csv_path'], sensor+".csv")
        logging.info("Updating counts for %s into %s" % (sensor, output_file))
        targets = SENSOR_COUNT_DEFINITIONS[sensor]

        # Load data frame from existing CSV or create a new one
        if os.path.exists(output_file):
            df = pd.read_csv(output_file)
            df.set_index('date')
        else:
            cols = ["date"]
            for target_count in targets:
                target_def = targets[target_count]

                cols.append(target_count)
                if "parent" in target_def:
                    cols.append(target_count+'%')

            df = pd.DataFrame(columns=cols)

        for current_date in dates_to_check:
            logging.info("[%s] %s" % (sensor, current_date))
            counts = {}
            percentages = {}

            # Populate count and percentage (if applicable) for each target count
            for target_count in targets:
                target_def = targets[target_count]
                counts[target_count] = perform_count(target_count, target_def, current_date, conn)
                if "parent" in target_def:
                    if target_def["parent"] not in counts:
                        counts[target_def["parent"]] = perform_count(targets[target_def["parent"]], current_date, conn)
                    if counts[target_def["parent"]] > 0:
                        percentages[target_count] = (counts[target_count]*1.0)/(counts[target_def["parent"]]*1.0)
                    else:
                        percentages[target_count] = 0.0

            # If this date already has a row, just update
            if current_date in df['date'].values:
                logging.info(current_date, 'is already in the table, updating...')
                for target_count in targets:
                    target_def = targets[target_count]
                    df.loc[df['date'] == current_date, target_count] = counts[target_count]
                    if "parent" in target_def:
                        df.loc[df['date'] == current_date, target_count+'%'] = percentages[target_count+'%']

            # If not, create a new row
            else:
                logging.info(current_date + 'is not already in the table, making new entry')
                new_entry = [current_date]
                indices = ["date"]

                for target_count in targets:
                    target_def = targets[target_count]

                    #indices.append(target_count)
                    new_entry.append(counts[target_count])
                    if "parent" in target_def:
                        indices.append(target_count+'%')
                        new_entry.append(percentages[target_count])
                        if current_date not in df['date'].values:
                            logging.info('adding new entry for date', current_date)
                            df.loc[len(df)] = new_entry
                        else:
                            logging.info('updating entry for date', current_date)
                            df.loc[df['date'] == current_date] = new_entry

        logging.info("Writing %s" % output_file)
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df.sort_values(by=['date'], inplace=True, ascending=True)
        df.to_csv(output_file, index=False)

    SCAN_LOCK = False

def main():
    thread.start_new_thread(run_update, ())

    apiIP = os.getenv('COUNTER_API_IP', "0.0.0.0")
    apiPort = os.getenv('COUNTER_API_PORT', "5454")
    app = create_app()
    logger.info("*** API now listening on %s:%s ***" % (apiIP, apiPort))
    app.run(host=apiIP, port=apiPort)

if __name__ == '__main__':

    config = loadJsonFile(os.path.join(app_dir, "config_default.json"))
    if os.path.exists(os.path.join(app_dir, "data/config_custom.json")):
        print("...loading configuration from config_custom.json")
        config = updateNestedDict(config, loadJsonFile(os.path.join(app_dir, "data/config_custom.json")))
    else:
        print("...no custom configuration file found. using default values")

    # Initialize logger handlers
    with open(os.path.join(app_dir, "config_logging.json"), 'r') as f:
        log_config = json.load(f)
        main_log_file = os.path.join(config["log_path"], "log_filecounter.txt")
        log_config['handlers']['file']['filename'] = main_log_file
        if not os.path.exists(config["log_path"]):
            os.makedirs(config["log_path"])
        if not os.path.isfile(main_log_file):
            open(main_log_file, 'a').close()
        logging.config.dictConfig(log_config)
    logger = logging.getLogger('counter')

    main()

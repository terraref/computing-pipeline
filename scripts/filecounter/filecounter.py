import os
import json
import thread
import collections
import time
import datetime
import psycopg2
import re
import requests
import logging, logging.config, logstash
import pandas as pd
from flask import Flask, render_template, send_file, request, url_for, redirect, make_response
from flask_wtf import FlaskForm as Form
from wtforms import TextField, TextAreaField, validators, StringField, SubmitField, DateField, SelectMultipleField, widgets
from wtforms.fields.html5 import DateField
from wtforms.validators import DataRequired

from pyclowder.connectors import Connector
from pyclowder.datasets import submit_extraction, get_file_list
from pyclowder.files import submit_extraction as submit_file_extraction
from terrautils.extractors import load_json_file
from terrautils.sensors import Sensors

import utils
import counts


config = {}
app_dir = '/home/filecounter'
SCAN_LOCK = False
count_defs = counts.SENSOR_COUNT_DEFINITIONS
DEFAULT_COUNT_START = None
DEFAULT_COUNT_END = None

CLOWDER_HOST = "https://terraref.ncsa.illinois.edu/clowder/"
CLOWDER_KEY = os.getenv('CLOWDER_KEY', False)
CONN = Connector("", {}, mounted_paths={"/home/clowder/sites":"/home/clowder/sites"})


# UTILITIES ----------------------------
def update_nested_dict(existing, new):
    """Nested update of python dictionaries for config parsing
    Adapted from http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    """
    for k, v in new.iteritems():
        if isinstance(existing, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = update_nested_dict(existing.get(k, {}), v)
                existing[k] = r
            else:
                existing[k] = new[k]
        else:
            existing = {k: new[k]}
    return existing

def generate_dates_in_range(start_date_string, end_date_string=None):
    """Return list of date strings between start and end dates."""
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

def get_percent_columns(current_dataframe):
    colnames = list(current_dataframe.columns.values)
    percent_columns = []
    for each in colnames:
        if each.endswith('%'):
            percent_columns.append(each)
    return percent_columns

def highlight_max(s):
    '''
    highlight the maximum in a Series yellow.
    '''
    is_max = s == s.max()
    return ['background-color: red' if v else '' for v in is_max]

def color_percents(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    if val == 100:
        color = 'green'
    elif val >= 99:
        color = 'greenyellow'
    elif val >= 95:
        color = 'yellow'
    else:
        color = 'lightcoral'
    return 'background-color: %s' % color

def render_date_entry(sensorname, columns, rowdata, rowindex):
    html = '<div><br/><a style="font-size:18px"><b>%s</b></a>' % rowdata['date']
    html += ' <a href="/newschedule/%s/%s/%s">(Recount)</a>' % (
        sensorname, rowdata['date'], rowdata['date'])
    html += '</br><table style="border: solid 2px;border-spacing:0px">'

    sensordef = count_defs[sensorname]
    vals = {}

    for colname in columns:
        if not colname.endswith("%"):
            if colname in sensordef:
                if colname not in vals:
                    vals[colname] = {}
                vals[colname]["count"] = rowdata[colname]
                if colname in sensordef and "parent" in sensordef[colname]:
                    vals[colname]["parent"] = sensordef[colname]["parent"]
        else:
            parcol = colname.replace("%", "")
            parname = sensordef[parcol]["parent"]
            if parcol not in vals:
                vals[parcol] = {}
            vals[parcol]["%"] = rowdata[colname]
            vals[parcol]["%str"] = "%s%% of %s" % (rowdata[colname], parname)

    for group in sensordef:
        api_link = ""
        if group != sensorname:
            group_cell = '<td style="border:solid 1px">...%s</td>' % group

            if sensordef[group]["type"] == "timestamp":
                if "%" in vals[group] and vals[group]["%"] < 100:
                    api_link = '<a href="/submitmissing/%s/%s/%s">Submit to %s</a>' % (
                        sensorname, group, rowdata['date'], sensordef[group]["extractor"])

            elif sensordef[group]["type"] == "psql":
                if "%" in vals[group] and vals[group]["%"] == 100:
                    api_link = '<a href="/submitrulecheck/%s/%s/%s">Retrigger ncsa.rulechecker.terra</a>' % (
                        sensorname, group, rowdata['date'])

                elif "%" in vals[group] and vals[group]["%"] < 100:
                    api_link = '<a href="/submitmissingrulechecks/%s/%s/%s">Submit to ncsa.rulechecker.terra</a>' % (
                        sensorname, group, rowdata['date'])

            elif sensordef[group]["type"] == "regex":
                if "parent" in sensordef[group]:
                    api_link = '<a href="/submitmissingregex/%s/%s/%s">Submit to %s</a>' % (
                        sensorname, group, rowdata['date'], sensordef[group]["extractor"])

            elif sensordef[group]["type"] == "plot":
                if "parent" in sensordef[group]:
                    api_link = '<a href="/submitmissingplots/%s/%s/%s">Submit to %s</a>' % (
                        sensorname, group, rowdata['date'], sensordef[group]["extractor"])

        else:
            group_cell = '<td style="border:solid 1px"><b>raw data</b></td>'
        api_cell = '<td style="border:solid 1px">%s</td>' % api_link

        if group in vals:
            if "%" in vals[group]:
                count_cell = '<td style="border:solid 1px;%s"><a title="%s">%s</a></td>' % (
                                                color_percents(vals[group]["%"]),
                                                vals[group]["%str"],
                                                vals[group]["count"])
            else:
                count_cell = '<td style="border:solid 1px"><a>%s</a></td>' % vals[group]["count"]
        else:
            count_cell = '<td style="border:solid 1px"><a>Missing</a></td>'

        html += '<tr>'
        html += group_cell
        html += count_cell
        html += api_cell
        html += '</tr>'
    html += '</table></div>'
    return html

def get_dsid_by_name(dsname):
    url = "%sapi/datasets?key=%s&title=%s&exact=true" % (CLOWDER_HOST, CLOWDER_KEY, dsname)
    result = requests.get(url)
    result.raise_for_status()

    if len(result.json()) > 0:
        ds_id = result.json()[0]['id']
        return ds_id
    else:
        return None

def connect_to_psql():
    psql_db = os.getenv("RULECHECKER_DATABASE", config['postgres']['database'])
    psql_host = os.getenv("RULECHECKER_HOST", config['postgres']['host'])
    psql_user = os.getenv("RULECHECKER_USER", config['postgres']['username'])
    psql_pass = os.getenv("RULECHECKER_PASSWORD", config['postgres']['password'])

    psql_conn = psycopg2.connect(dbname=psql_db, user=psql_user, host=psql_host, password=psql_pass)

    return psql_conn

# FLASK COMPONENTS ----------------------------
def create_app(test_config=None):

    pipeline_csv = os.path.join(config['csv_path'], "{}.csv")

    sensor_names = count_defs.keys()

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

    class MultiCheckboxField(SelectMultipleField):
        widget = widgets.ListWidget(prefix_label=False)
        option_widget = widgets.CheckboxInput()

    class SensorDateSelectForm(Form):
        sensor_names = count_defs.keys()
        selects = [(x, x) for x in sensor_names]
        sensors = MultiCheckboxField('Label', choices=selects)
        start_date = DateField('Start', format='%Y-%m-%d', validators=[DataRequired()])
        end_date = DateField('End', format='%Y-%m-%d')
        submit = SubmitField('Count files for these days', validators=[DataRequired()])

    @app.route('/sensors', defaults={'message': "Available Sensors and Options"})
    @app.route('/sensors/<string:message>')
    def sensors(message):
        return render_template('sensors.html', sensors=sensor_names, message=message)

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
        if not os.path.isfile(current_csv):
            return "File does not exist"
        df = pd.read_csv(current_csv, index_col=False)
        if days == 0:
            percent_columns = get_percent_columns(df)
            for each in percent_columns:
                df[each] = df[each].mul(100).astype(int)
            dfs = df.style
            dfs.applymap(color_percents, subset=percent_columns).set_table_attributes("border=1")
            my_html = dfs.render()
            return my_html
        else:
            return df.tail(days).to_html()

    @app.route('/showcsvbyseason/<sensor_name>', defaults={'season': 6})
    @app.route('/showcsvbyseason/<sensor_name>/<int:season>')
    def showcsvbyseason(sensor_name, season):
        if season == 6:
            start = '2018-04-06'
            end = '2018-08-01'
            current_csv = pipeline_csv.format(sensor_name)
            df = pd.read_csv(current_csv, index_col=False)
            df_season = df.loc[(df['date'] >= start) & (df['date'] <= end)]

            # Omit rows with zero count in raw_data
            for sensorname in ['stereoTop', 'flirIrCamera', 'scanner3DTop']:
                if sensorname in df_season.columns:
                    df_season = df_season[df[sensorname] != 0]

            percent_columns = get_percent_columns(df_season)
            for each in percent_columns:
                df_season[each] = df_season[each].mul(100).astype(int)

            dfs = df_season.style
            dfs.applymap(color_percents, subset=percent_columns).set_table_attributes("border=1")
            html = dfs.render()
            return html

        else:
            current_csv = pipeline_csv.format(sensor_name)
            df = pd.read_csv(current_csv, index_col=False)
            percent_columns = get_percent_columns(df)
            for each in percent_columns:
                df[each] = df[each].mul(100).astype(int)
            dfs = df.style
            dfs.applymap(color_percents, subset=percent_columns).set_table_attributes("border=1")
            my_html = dfs.render()
            return my_html

    @app.route('/resubmitbyseason/<sensor_name>', defaults={'season': 6})
    @app.route('/resubmitbyseason/<sensor_name>/<int:season>')
    def resubmitbyseason(sensor_name, season):
        if season == 6:
            start = '2018-04-06'
            end = '2018-08-01'
            current_csv = pipeline_csv.format(sensor_name)
            df = pd.read_csv(current_csv, index_col=False)
            df_season = df.loc[(df['date'] >= start) & (df['date'] <= end)]

            # Omit rows with zero count in raw_data
            primary_sensor = None
            for sensorname in ['stereoTop', 'flirIrCamera', 'scanner3DTop', 'ps2Top', 'EnvironmentLogger']:
                if sensorname in df_season.columns:
                    df_season = df_season[df[sensorname] != 0]
                    primary_sensor = sensorname

            percent_columns = get_percent_columns(df_season)
            for each in percent_columns:
                df_season[each] = df_season[each].mul(100).astype(int)

            # Create header and key
            html = "<h1>Seasonal Counts: %s</h1><div>" % primary_sensor
            html += '<a style="%s">%s</a></br>' % (color_percents(100),' 100% coverage')
            html += '<a style="%s">%s</a></br>' % (color_percents(99), '>=99% coverage')
            html += '<a style="%s">%s</a></br>' % (color_percents(98), '>=95% coverage')
            html += '<a style="%s">%s</a></br></br>' % (color_percents(0),  ' <95% coverage')

            # Create daily entries
            cols = list(df_season.columns.values)
            for index, row in df_season.iterrows():
                html += render_date_entry(primary_sensor, cols, row, index)
            html += "</div>"

            return html

        else:
            current_csv = pipeline_csv.format(sensor_name)
            df = pd.read_csv(current_csv, index_col=False)
            percent_columns = get_percent_columns(df)
            for each in percent_columns:
                df[each] = df[each].mul(100).astype(int)
            dfs = df.style
            dfs.applymap(color_percents, subset=percent_columns).set_table_attributes("border=1")
            my_html = dfs.render()
            return my_html

    @app.route('/submitmissing/<sensor_name>/<target>/<date>')
    @utils.requires_user("admin")
    def submit_missing_timestamps(sensor_name, target, date):
        sensordef = count_defs[sensor_name]
        targetdef = sensordef[target]
        extractorname = targetdef["extractor"]
        submitted = []
        notfound = []

        if "parent" in targetdef:
            # Count expected parent counts & actual current progress counts from filesystem
            parentdef = sensordef[targetdef["parent"]]
            parent_dir = os.path.join(parentdef["path"], date)
            target_dir = os.path.join(targetdef["path"], date)
            parent_timestamps = os.listdir(parent_dir)
            if os.path.isdir(target_dir):
                target_timestamps = os.listdir(target_dir)
            else:
                target_timestamps = []

            disp_name = Sensors("", "ua-mac").get_display_name(targetdef["parent"])
            missing = list(set(parent_timestamps)-set(target_timestamps))
            for ts in missing:
                if ts.find("-") > -1 and ts.find("__") > -1:
                    dataset_name = disp_name+" - "+ts
                    raw_dsid = get_dsid_by_name(dataset_name)
                    if raw_dsid:
                        submit_extraction(CONN, CLOWDER_HOST, CLOWDER_KEY, raw_dsid, extractorname)
                        submitted.append({"name": dataset_name, "id": raw_dsid})
                    else:
                        notfound.append({"name": dataset_name})

        return json.dumps({
            "extractor": extractorname,
            "datasets submitted": submitted,
            "datasets not found": notfound
        })

    @app.route('/submitrulecheck/<sensor_name>/<target>/<date>')
    @utils.requires_user("admin")
    def submit_rulecheck(sensor_name, target, date):
        sensordef = count_defs[sensor_name]
        targetdef = sensordef[target]
        submitted = []

        s = Sensors("", "ua-mac")

        if "parent" in targetdef:
            target_dir = os.path.join(sensordef[targetdef["parent"]]["path"], date)
            target_timestamps = os.listdir(target_dir)

            disp_name = s.get_display_name(targetdef["parent"])

            for ts in target_timestamps:
                if ts.find("-") > -1 and ts.find("__") > -1: # TODO: and os.listdir(os.path.join(target_dir, ts)):
                    # Get first populated timestamp for the date that has a Clowder ID
                    dataset_name = disp_name+" - "+ts
                    raw_dsid = get_dsid_by_name(dataset_name)
                    if raw_dsid:
                        # Submit associated Clowder ID to rulechecker
                        submit_extraction(CONN, CLOWDER_HOST, CLOWDER_KEY, raw_dsid, "ncsa.rulechecker.terra")
                        submitted.append({"name": dataset_name, "id": raw_dsid})
                        break

        return json.dumps({
            "extractor": "ncsa.rulechecker.terra",
            "datasets submitted": submitted
        })

    @app.route('/submitmissingrulechecks/<sensor_name>/<target>/<date>')
    @utils.requires_user("admin")
    def submit_missing_timestamps_from_rulechecker(sensor_name, target, date):
        sensordef = count_defs[sensor_name]
        targetdef = sensordef[target]
        extractorname = targetdef["extractor"]
        submitted = []
        notfound = []

        if "parent" in targetdef:
            # Count expected parent counts from filesystem
            parentdef = sensordef[targetdef["parent"]]
            parent_dir = os.path.join(parentdef["path"], date)
            parent_timestamps = os.listdir(parent_dir)

            # Count actual current progress counts from PSQL
            psql_conn = connect_to_psql()

            target_timestamps = []
            query_string = targetdef["query_list"] % date
            curs = psql_conn.cursor()
            curs.execute(query_string)
            for result in curs:
                target_timestamps.append(result[0].split("/")[-2])

            disp_name = Sensors("", "ua-mac").get_display_name(targetdef["parent"])
            missing = list(set(parent_timestamps)-set(target_timestamps))
            for ts in missing:
                if ts.find("-") > -1 and ts.find("__") > -1:
                    dataset_name = disp_name+" - "+ts
                    raw_dsid = get_dsid_by_name(dataset_name)
                    if raw_dsid:
                        submit_extraction(CONN, CLOWDER_HOST, CLOWDER_KEY, raw_dsid, extractorname)
                        submitted.append({"name": dataset_name, "id": raw_dsid})
                    else:
                        notfound.append({"name": dataset_name})

        return json.dumps({
            "extractor": extractorname,
            "datasets submitted": submitted,
            "datasets not found": notfound
        })

    @app.route('/submitmissingregex/<sensor_name>/<target>/<date>')
    @utils.requires_user("admin")
    def submit_missing_regex(sensor_name, target, date):
        sensordef = count_defs[sensor_name]
        targetdef = sensordef[target]
        extractorname = targetdef["extractor"]
        submitted = []
        notfound = []

        if "parent" in targetdef:
            # Count expected parent counts from filesystem
            parentdef = sensordef[targetdef["parent"]]
            parent_dir = os.path.join(parentdef["path"], date)

            if parentdef["type"] == "regex" and parentdef["path"] == targetdef["path"]:
                for file in os.listdir(parent_dir):
                    if re.match(parentdef["regex"], file):
                        expected_output = file.replace(targetdef["parent_replacer_check"][1],
                                                       targetdef["parent_replacer_check"][0])
                        if not os.path.isfile(os.path.join(parent_dir, expected_output)):
                            # Find the file ID of the parent file and submit it
                            dataset_name = parentdef["dispname"]+" - "+date
                            dsid = get_dsid_by_name(dataset_name)
                            if dsid:
                                parent_id = None
                                dsfiles = get_file_list(CONN, CLOWDER_HOST, CLOWDER_KEY, dsid)
                                matchfile = file.replace("_thumb.tif", ".tif")
                                for dsfile in dsfiles:
                                    if dsfile["filename"] == matchfile:
                                        parent_id = dsfile["id"]
                                        break
                                if parent_id:
                                    submit_file_extraction(CONN, CLOWDER_HOST, CLOWDER_KEY, parent_id, extractorname)
                                    submitted.append({"name": matchfile, "id": parent_id})
                                else:
                                    notfound.append({"name": matchfile})
                            else:
                                notfound.append({"name": dataset_name})

        return json.dumps({
            "extractor": extractorname,
            "datasets submitted": submitted,
            "datasets not found": notfound
        })

    @app.route('/submitmissingplots/<sensor_name>/<target>/<date>')
    @utils.requires_user("admin")
    def submit_missing_plots(sensor_name, target, date):
        sensordef = count_defs[sensor_name]
        targetdef = sensordef[target]
        extractorname = targetdef["extractor"]
        submitted = []
        notfound = []

        if "parent" in targetdef:
            # Count expected parent counts from filesystem
            parentdef = sensordef[targetdef["parent"]]
            parent_dir = os.path.join(parentdef["path"], date)
            target_dir = os.path.join(targetdef["path"], date)
            parent_plots = os.listdir(parent_dir)
            if os.path.isdir(target_dir):
                target_plots = os.listdir(target_dir)
            else:
                target_plots = []

        return json.dumps({
            "extractor": extractorname,
            "datasets submitted": submitted,
            "datasets not found": notfound
        })

    @app.route('/dateoptions', methods=['POST','GET'])
    @utils.requires_user("admin")
    def dateoptions():
        form = SensorDateSelectForm(request.form)
        if form.validate_on_submit():
            raw_selected_sensors = form.sensors.data
            for r in raw_selected_sensors:
                # TODO: Currently only one sensor can be scheduled at a time
                return redirect(url_for('schedule_count',
                                    sensor=str(r),
                                    start_range=str(form.start_date.data.strftime('%Y-%m-%d')),
                                    end_range=str(form.end_date.data.strftime('%Y-%m-%d'))))
        return render_template('dateoptions.html', form=form)

    @app.route('/archive')
    def archive():
        sensor_list = count_defs.keys()
        current_time_stamp = str(datetime.datetime.now()).replace(' ', '_')
        for sensor in sensor_list:
            output_file = os.path.join(config['csv_path'], sensor + ".csv")
            if os.path.exists(output_file):
                archived_file = os.path.join(config['csv_path'], sensor + '_' + current_time_stamp + ".csv")
                os.rename(output_file, archived_file)
                if os.path.exists(output_file):
                    try:
                        os.remove(output_file)
                    except OSError as e:
                        logging.info(e)
        message = "Archived existing count csvs"
        logging.info("Archived existing count csvs")
        return redirect(url_for('sensors', message=message))

    @app.route('/newschedule/<sensor>/<start_range>/<end_range>')
    @utils.requires_user("admin")
    def schedule_count(sensor, start_range, end_range):
        dates_in_range = generate_dates_in_range(start_range, end_range)
        psql_conn = connect_to_psql()
        thread.start_new_thread(update_file_count_csvs, (sensor, dates_in_range, psql_conn))

        message = "Custom scan scheduled for %s on %s dates" % (sensor, len(dates_in_range))
        return redirect(url_for('sensors', message=message))

    return app


# COUNTING COMPONENTS ----------------------------
def run_regular_update(use_defaults=False):
    """Perform regular update of previous two weeks for all sensors"""
    psql_conn = connect_to_psql()

    while True:
        # Determine two weeks before current date, or by defaults
        if use_defaults:
            logging.info("Using default values instead of previous 2 weeks")
            start_date_string = DEFAULT_COUNT_START
            end_date_string = DEFAULT_COUNT_END
            dates_to_check = generate_dates_in_range(start_date_string, end_date_string)
        else:
            today = datetime.datetime.now()
            two_weeks = today - datetime.timedelta(days=14)
            start_date_string = os.getenv('START_SCAN_DATE', two_weeks.strftime("%Y-%m-%d"))
            dates_to_check = generate_dates_in_range(start_date_string)

        logging.info("Checking counts for all sensors for dates %s - %s" % (start_date_string, dates_to_check[-1]))

        for s in count_defs.keys():
            psql_conn = update_file_count_csvs(s, dates_to_check, psql_conn)

        # Wait 1 hour for next iteration
        time.sleep(3600)

def retrive_single_count(target_count, target_def, date, psql_conn):
    """Return count of specified type (see counts.py for types)"""
    count = 0

    if target_def["type"] == "timestamp":
        date_dir = os.path.join(target_def["path"], date)
        if os.path.exists(date_dir):
            logging.info("   [%s] counting timestamps in %s" % (target_count, date_dir))
            # TODO: Only count non-empty directories
            """count = 0
            for sub in os.listdir(date_dir):
                if os.listdir(os.path.join(date_dir, sub)):
                    count += 1"""
            count = len(os.listdir(date_dir))
        else:
            logging.info("   [%s] directory not found: %s" % (target_count, date_dir))

    elif target_def["type"] == "plot":
        date_dir = os.path.join(target_def["path"], date)
        if os.path.exists(date_dir):
            logging.info("   [%s] counting plots in %s" % (target_count, date_dir))
            # TODO: Only count non-empty directories
            """count = 0
            for sub in os.listdir(date_dir):
                if os.listdir(os.path.join(date_dir, sub)):
                    count += 1"""
            count = len(os.listdir(date_dir))
        else:
            logging.info("   [%s] directory not found: %s" % (target_count, date_dir))

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
        else:
            logging.info("   [%s] directory not found: %s" % (target_count, date_dir))

    elif target_def["type"] == "psql":
        logging.info("   [%s] querying PSQL records for %s" % (target_count, date))
        query_string = target_def["query_count"] % date
        curs = psql_conn.cursor()
        curs.execute(query_string)
        for result in curs:
            count = result[0]

    return count

def update_file_count_csvs(sensor, dates_to_check, psql_conn):
    """Perform necessary counting on specified dates to update CSV for all sensors."""
    global SCAN_LOCK

    while SCAN_LOCK:
        logging.info("Another thread currently locking database; waiting 60 seconds to retry")
        time.sleep(60)

    logging.info("Locking scan for %s on %s dates" % (sensor, len(dates_to_check)))
    SCAN_LOCK = True

    output_file = os.path.join(config['csv_path'], sensor+".csv")
    logging.info("Updating counts for %s into %s" % (sensor, output_file))
    targets = count_defs[sensor]
    cols = ["date"]
    for target_count in targets:
        target_def = targets[target_count]
        cols.append(target_count)
        if "parent" in target_def:
            cols.append(target_count + '%')

    # Load data frame from existing CSV or create a new one
    if os.path.exists(output_file):
        logging.info("csv exists for %s" % output_file)
        try:
            df = pd.read_csv(output_file)
        except Exception as e:
            logging.info(e)
            logging.info('CSV exists, could not read as dataframe')
            cols = ["date"]
            df = pd.DataFrame(columns=cols)
            logging.info("CSV existed but could not be read, created dataframe for %s " % sensor)
        df_columns = list(df.columns.values)
        if df_columns != cols:
            logging.info("CSV existed but had malformed columns, created dataframe for %s " % sensor)
            df = pd.DataFrame(columns=cols)
    else:
        logging.info("output file for %s does not exist" % sensor)
        df = pd.DataFrame(columns=cols)
        logging.info("CSV did not exist, created dataframe for %s " % sensor)

    # Populate count and percentage (if applicable) for each target count
    logging.info("the columns of the csv are %s " % str(df.columns.values))
    for current_date in dates_to_check:
        logging.info("[%s] %s" % (sensor, current_date))
        counts = {}
        percentages = {}
        for target_count in targets:
            target_def = targets[target_count]

            try:
                counts[target_count] = retrive_single_count(target_count, target_def, current_date, psql_conn)
            except:
                psql_conn = connect_to_psql()
                counts[target_count] = retrive_single_count(target_count, target_def, current_date, psql_conn)

            if "parent" in target_def:
                if target_def["parent"] not in counts:
                    counts[target_def["parent"]] = retrive_single_count(targets[target_def["parent"]], current_date, psql_conn)
                if counts[target_def["parent"]] > 0:
                    percentages[target_count] = (counts[target_count]*1.0)/(counts[target_def["parent"]]*1.0)
                else:
                    percentages[target_count] = 0.0

        # If this date already has a row, just update
        if current_date in df['date'].values:
            logging.info("Already have data for date %s " % current_date)
            updated_entry = [current_date]
            for target_count in targets:
                target_def = targets[target_count]
                updated_entry.append(counts[target_count])
                if "parent" in target_def:
                    updated_entry.append(percentages[target_count])
            df.loc[df['date'] == current_date] = updated_entry
        # If not, create a new row
        else:
            logging.info("No data for date %s adding to dataframe" % current_date)
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

    logging.info("Writing %s" % output_file)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df.sort_values(by=['date'], inplace=True, ascending=True)
    df.to_csv(output_file, index=False)

    SCAN_LOCK = False
    return psql_conn


if __name__ == '__main__':

    logger = logging.getLogger('counter')

    config = load_json_file(os.path.join(app_dir, "config_default.json"))

    if os.path.exists(os.path.join(app_dir, "data/config_custom.json")):
        print("...loading configuration from config_custom.json")
        config = update_nested_dict(config, load_json_file(os.path.join(app_dir, "data/config_custom.json")))
        try:
            DEFAULT_COUNT_START = str(config["default_count_start"])
            DEFAULT_COUNT_END = str(config["default_count_end"])
            print(DEFAULT_COUNT_START, DEFAULT_COUNT_END)
            print("default start and end provided")
        except:
            print("No default values for start and end")
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

    thread.start_new_thread(run_regular_update, (True,))

    apiIP = os.getenv('COUNTER_API_IP', "0.0.0.0")
    apiPort = os.getenv('COUNTER_API_PORT', "5454")
    app = create_app()
    logger.info("*** API now listening on %s:%s ***" % (apiIP, apiPort))
    app.run(host=apiIP, port=apiPort)

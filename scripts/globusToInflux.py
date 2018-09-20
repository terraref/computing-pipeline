import requests
import json
import datetime
import os
from dateutil.parser import parse
from influxdb import InfluxDBClient, SeriesHelper

globus_url = 'https://terramonitor.workbench.terraref.org/status'

def add_arguments(parser):

    parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu"),
                        help="InfluxDB URL for logging")
    parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
                        default= os.getenv("INFLUXDB_PORT", 8086),
                        help="InfluxDB port")
    parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_USER", "terra"),
                        help="InfluxDB username")
    parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_PASSWORD", ''),
                        help="InfluxDB password")
    parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
                        default=os.getenv("INFLUXDB_DB", "extractor_db"),
                        help="InfluxDB database")

def get_globus_status():
    r = requests.get(globus_url)
    result = r.json()
    return result

class Influx():

    def __init__(self, host, port, db, user, pass_):

        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pass_ = pass_


    def log_globus_status(self, extractorname, starttime, endtime, filecount, bytecount):

        curr_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        # sample string = {'IN PROGRESS': 101, 'RETRY': 0, 'SUCCEEDED': 0, 'PROCESSED': 513335, 'ERROR': 16, 'PENDING': 4}
        current_status = get_globus_status()

        in_progress = current_status['IN PROGRESS']
        retry = current_status['RETRY']
        processed = current_status['PROCESSED']
        error = current_status['ERROR']
        pending = current_status['PENDING']


        if self.pass_:
            client = InfluxDBClient(self.host, self.port, self.user,
                                    self.pass_, self.db)

            client.write_points([{
                "measurement": "in_progress",
                "time": curr_time,
                "fields": {"value": int(in_progress)}
            }], tags={"type": "in_progress"})
            client.write_points([{
                "measurement": "retry",
                "time": curr_time,
                "fields": {"value": int(retry)}
            }], tags={"type": "retry"})
            client.write_points([{
                "measurement": "processed",
                "time": curr_time,
                "fields": {"value": int(processed)}
            }], tags={"type": "processed"})
            client.write_points([{
                "measurement": "error",
                "time": curr_time,
                "fields": {"value": int(error)}
            }], tags={"type": "error"})
            client.write_points([{
                "measurement": "pending",
                "time": curr_time,
                "fields": {"value": int(pending)}
            }], tags={"type": "pending"})

    def error(self):
        # TODO: Allow sending critical error notification, e.g. email or Slack?
        pass


#get_globus_status()


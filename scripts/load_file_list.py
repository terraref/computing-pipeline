## Load contents of Condo file dump into Postgres


import os
import logging
import psycopg2


def connectToPostgres():
    """
    If rulemonitor database does not exist yet:
        $ initdb /home/rulemonitor/postgres/data
        $ pg_ctl -D /home/rulemonitor/postgres/data -l /home/rulemonitor/postgres/log
        $   createdb rulemonitor
    """

    pghost = os.getenv("POSTGRES_HOST", "192.168.5.83")
    pguser = os.getenv("POSTGRES_USER", "rulemonitor")
    pgpass = os.getenv("POSTGRES_PASSWORD", "BSJYngTW4k")

    conn = psycopg2.connect(dbname='rulemonitor', user=pguser, host=pghost, password=pgpass)
    logging.info("Connected to Postgres")
    return conn

def parse_line(linestr):
    # 1114061452 2051065010 0  32 4258 54 54 160 54 202 47852 -- /terraref/sites/ua-mac/raw_data/stereoTop/2017-08-24/2017-08-24__12-40-01-916/ef0b016f-3069-4f00-90fb-95475cb581f6_metadata.json
    # 1114061470 593171087 0  7968 8147712 54 54 254 54 202 47852 -- /terraref/sites/ua-mac/raw_data/stereoTop/2017-05-22/2017-05-22__14-47-17-826/d83f0811-de9d-4406-8707-26fabfd2e845_right.bin
    # 1114061482 84336251 0  7968 8147712 54 54 160 54 202 47852 -- /terraref/sites/ua-mac/raw_data/stereoTop/2017-08-24/2017-08-24__12-40-01-916/ef0b016f-3069-4f00-90fb-95475cb581f6_right.bin

    (data, filepath) = linestr.split("--")
    filepath = filepath.strip().replace("/terraref/sites/ua-mac/", "/sites/ua-mac")
    sub = data.split(" ")

    return {
        "filepath": filepath,
        "filename": os.path.basename(filepath),
        "filesize": sub[5],
        "create_time": sub[6],
        "change_time": sub[7],
        "mod_time": sub[8],
        "access_time": sub[9],
        "GID": sub[10],
        "UID": sub[11]
    }

def post_line(curs, linedata):
    q_insert = "INSERT INTO filesystem VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    q_data = (linedata["filepath"], linedata["filename"], linedata["filesize"], linedata["create_time"], linedata["change_time"],
        linedata["mod_time"], linedata["access_time"], linedata["GID"], linedata["UID"])
    curs.execute(q_insert, q_data)


input_file = "/home/mburnet2/filedb/all-files.txt"

"""
CREATE TABLE filesystem (filepath TEXT, filename TEXT, filesize BIGINT, create_time INT, change_time INT, mod_time INT, access_time INT, gid TEXT, uid TEXT);
"""

conn = connectToPostgres()
curs = conn.cursor()
lines = 0
with open(input_file) as f:
    curr_line = f.readline()
    while curr_line:
        curr_line = f.readline()
        line_data = parse_line(curr_line)
        post_line(curs, line_data)
        lines += 1

        if lines % 10000 == 0:
            logging.info("inserted %s lines" % lines)
            conn.commit()

    conn.commit()
curs.close()
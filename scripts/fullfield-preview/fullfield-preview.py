import os
import logging
import time
import datetime
import psycopg2
import re
from collections import OrderedDict
from flask import Flask, render_template, send_file, request, url_for, redirect, make_response
from flask_wtf import FlaskForm as Form
from wtforms import TextField, TextAreaField, validators, StringField, SubmitField, DateField
from wtforms.fields.html5 import DateField
from wtforms.validators import DataRequired

config = {}

app_dir = "/home/fullfield-preview/"
sites_root = "/home/clowder/"

PEOPLE_FOLDER = os.path.join('static', 'images')

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    app.config['UPLOAD_FOLDER'] = PEOPLE_FOLDER

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

    @app.route('/test')
    def test():
        full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'test-plant-image.jpg')

        return render_template("show_image.html", user_image=full_filename)
        #return 'this is only a test'

    return app

def main():

    apiIP = os.getenv('FULLFIELD_PREVIEW_API_IP', "0.0.0.0")
    apiPort = os.getenv('FULLFIELD_PREVIEW_API_PORT', "5454")
    app = create_app()
    logger.info("*** API now listening on %s:%s ***" % (apiIP, apiPort))
    app.run(host=apiIP, port=apiPort)

if __name__ == '__main__':

    logger = logging.getLogger('counter')

    if os.path.exists(os.path.join(app_dir, "data/config_custom.json")):
        print("...loading configuration from config_custom.json")
    else:
        print("...no custom configuration file found. using default values")

    # Initialize logger handlers
    # with open(os.path.join(app_dir, "config_logging.json"), 'r') as f:
    #     log_config = json.load(f)
    #     main_log_file = os.path.join(config["log_path"], "log_filecounter.txt")
    #     log_config['handlers']['file']['filename'] = main_log_file
    #     if not os.path.exists(config["log_path"]):
    #         os.makedirs(config["log_path"])
    #     if not os.path.isfile(main_log_file):
    #         open(main_log_file, 'a').close()
    #     logging.config.dictConfig(log_config)


    main()
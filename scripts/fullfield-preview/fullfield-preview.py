import os
import logging
import tempfile
import shutil
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
from flask import Flask, render_template, send_file, request, url_for, redirect, make_response
from PIL import Image



config = {}

app_dir = "/home/fullfield-preview/"
sites_root = "/home/clowder/"

ir_fullfield_dir = '/ua-mac/Level_2/ir_fullfield/'

PEOPLE_FOLDER = os.path.join('static', 'images')


def scale_image(input_image_path,
                output_image_path,
                width=None,
                height=None
                ):
    original_image = Image.open(input_image_path)
    w, h = original_image.size
    print('The original image size is {wide} wide x {height} '
          'high'.format(wide=w, height=h))

    if width and height:
        max_size = (width, height)
    elif width:
        max_size = (width, h)
    elif height:
        max_size = (w, height)
    else:
        # No width or height specified
        raise RuntimeError('Width or height required!')

    original_image.thumbnail(max_size, Image.ANTIALIAS)
    original_image.save(output_image_path)

    scaled_image = Image.open(output_image_path)
    width, height = scaled_image.size
    print('The scaled image size is {wide} wide x {height} '
          'high'.format(wide=width, height=height))

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

    # No caching at all for API endpoints.
    @app.after_request
    def add_header(response):
        # response.cache_control.no_store = True
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post - check = 0, pre - check = 0, max - age = 0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response
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

    class ExampleForm(Form):
        selected_date = DateField('Start', format='%Y-%m-%d', validators=[DataRequired()])
        submit = SubmitField('Show Available Fullfields', validators=[DataRequired()])

    @app.route('/')
    def test():
        full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'monolith_2001.jpg')
        scaled_image_filename_100 = os.path.join(app.config['UPLOAD_FOLDER'], 'resized_image_100.jpg')
        scaled_image_filename_300 = os.path.join(app.config['UPLOAD_FOLDER'], 'resized_image_300.jpg')

        t_100 = tempfile.NamedTemporaryFile(dir=os.path.join(app.config['UPLOAD_FOLDER']), suffix='.jpg')
        t_300 = tempfile.NamedTemporaryFile(dir=os.path.join(app.config['UPLOAD_FOLDER']), suffix='.jpg' )

        if os.path.isfile(scaled_image_filename_100):
            os.remove(scaled_image_filename_100)
        if os.path.isfile(scaled_image_filename_300):
            os.remove(scaled_image_filename_300)

        scale_image(full_filename,t_100, width=100)
        scale_image(full_filename, t_300, width=300)
        shutil.copy(t_100.name, scaled_image_filename_100)
        shutil.copy(t_300.name, scaled_image_filename_300)

        return render_template("show_image.html", user_image=full_filename, resized_image=scaled_image_filename_100, resized_image_2 =scaled_image_filename_300)
        #resp.cache_control.no_cache = True
        #return resp
        #return 'this is only a test'

    @app.route('/dateoptions', methods=['POST','GET'])
    def dateoptions():
        form = ExampleForm(request.form)
        if form.validate_on_submit():
            return redirect(url_for('show_fullfield',
                                    selected_date=str(form.selected_date.data.strftime('%Y-%m-%d'))))
            return render_template('dateoptions.html', form=form)
        return render_template('dateoptions.html', form=form)

    @app.route('/show_fullfield/<selected_date>')
    def show_fullfield(selected_date):
        ir_fullfield_dir_for_date = ir_fullfield_dir + selected_date + '/'
        files_in_dir = os.listdir(ir_fullfield_dir_for_date)
        ir_fullfield_thumbnails = []
        for f in files_in_dir:
            if f.endswith('_thumb.tif') or f.endswith('_thumb.tiff'):
                if 'rgb' in f:
                    ir_fullfield_thumbnails.append(f)
        return 'this is the thumbnails :  ' + ir_fullfield_thumbnails

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
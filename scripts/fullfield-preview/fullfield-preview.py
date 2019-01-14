import os
import logging
import tempfile
import shutil
import time
import datetime
import psycopg2
import re
from collections import OrderedDict
import flask
from flask import Flask, session, render_template, send_file, request, url_for, redirect, make_response
from flask_session import Session

from flask_wtf import FlaskForm as Form
from wtforms import TextField, TextAreaField, validators, StringField, SubmitField, DateField
from wtforms.fields.html5 import DateField
from wtforms.validators import DataRequired
from flask import session
#from flask.ext.session import Session
from flask import Flask, render_template, send_file, request, url_for, redirect, make_response
from wtforms.fields.html5 import DecimalRangeField, IntegerRangeField
from PIL import Image

import errno


def copy(src, dest):
    try:
        shutil.copytree(src, dest)
    except OSError as e:
        # If the error was caused because the source wasn't a directory
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            print('Directory not copied. Error: %s' % e)



config = {}

app_dir = "/home/fullfield-preview/"
sites_root = "/home/clowder/"

ir_fullfield_dir = '/ua-mac/Level_2/ir_fullfield/'

fullfield_thumbnails_directory = '/Users/helium/terraref-globus/thumbnails/'
LOCAL_THUMBNAIL_DIRECTORY = os.path.join('static', 'images', 'local-thumbnails')

PEOPLE_FOLDER = os.path.join('static', 'images')

five_item_list = ['apple', 'banana', 'cranberry', 'date', 'eggplant']


class TestForm(Form):
    day = IntegerRangeField('Day', default=0)

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

def get_daterange_for_season(season):
    print('getting date range for seasons : ' + season)

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

    app.config['SESSION_TYPE'] = 'redis'

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
    app.config['LOCAL_THUMBNAILS'] = LOCAL_THUMBNAIL_DIRECTORY

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

    @app.route('/',methods=['POST','GET'])
    def index():
        session['key'] = 'value'
        print(session, 'is the session')
        if 'username' in session:
            print('username is in session')
            username = session['username']
            return 'Logged in as ' + username + '<br>' + \
                   "<b><a href = '/logout'>click here to log out</a></b>"

        return "You are not logged in <br><a href = '/login'></b>" + \
           "click here to log in</b></a>"

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        print('doing login', request.method)
        if request.method == 'POST':
            print(request.form['username'], 'in post')
            session['username'] = request.form['username']
            return redirect(url_for('index'))
        return render_template('login.html')

    @app.route('/testlogin', methods=['GET', 'POST'])
    def testlogin():
        if request.method == 'POST':
            print('we are posting')
            username = request.form['username']
            result = 'post' + ' ' + username
            return result

    @app.route('/sessionstuff')
    def sessionstuff():
        # remove the username from the session if it is there
        print(session['key'])
        return 'this is to test session'

    @app.route('/logout')
    def logout():
        # remove the username from the session if it is there
        session.pop('username', None)
        return redirect(url_for('index'))

    @app.route('/test')
    def test():#
        full_filename = os.path.join(app.config['UPLOAD_FOLDER'], 'monolith_2001.jpg')
        #full_filename = os.path.join(app.config['UPLOAD_FOLDER'],'thumbnails','temporary','fullfield_L1_ua-mac_2017-01-01_rgb_thumb.png')

        scaled_image_filename_100 = os.path.join(app.config['UPLOAD_FOLDER'], 'resized_image_100.jpg')
        scaled_image_filename_300 = os.path.join(app.config['UPLOAD_FOLDER'], 'resized_image_300.jpg')

        new_full_filename = '/Users/helium/Desktop/unicorn.jpg'


        t_100 = tempfile.NamedTemporaryFile(dir=os.path.join(app.config['UPLOAD_FOLDER']), suffix='.jpg')
        t_300 = tempfile.NamedTemporaryFile(dir=os.path.join(app.config['UPLOAD_FOLDER']), suffix='.jpg' )

        if os.path.isfile(scaled_image_filename_100):
            os.remove(scaled_image_filename_100)
        if os.path.isfile(scaled_image_filename_300):
            os.remove(scaled_image_filename_300)

        scale_image(full_filename,t_100, width=100)
        scale_image(full_filename, t_300, width=300)
        shutil.copy(new_full_filename, scaled_image_filename_100)
        shutil.copy(t_300.name, scaled_image_filename_300)

        return render_template("show_image.html", user_image=full_filename, resized_image=scaled_image_filename_100, resized_image_2 =scaled_image_filename_300)

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

    @app.route('/select')
    def select():
        available_seasons = [1, 2, 3, 4, 5, 6]
        return render_template('main_selection.html', seasons=available_seasons)

    @app.route('/display_season/', methods=['GET', 'POST'])
    def display_season():
        select = request.form.get('season_select')
        message = "we are finding dates for seasons : " + str(select)
        flask.session['count'] = 0
        form = TestForm(csrf_enabled=False)
        slider_val_default = 0
        return render_template('display_season.html', message=message, form=form, image_list=five_item_list, slider_val=slider_val_default)

    @app.route('/preview_season', methods=['GET','POST'])
    def preview_season():
        select = request.form.get('season_select')
        copy(fullfield_thumbnails_directory, LOCAL_THUMBNAIL_DIRECTORY)
        files = app.config['LOCAL_THUMBNAILS']
        print("all the files are")
        print(files)
        '''function to return the HTML page to display the images'''
        flask.session['count'] = 0
        _files = files
        current_file = os.path.join(app.config['LOCAL_THUMBNAILS'], _files[0])
        current_filename = _files[0]
        message = "we are finding dates for seasons : " + str(select)
        return flask.render_template('season_display.html', photo=current_file,file_name=current_filename, current_season=select, message=message)

    @app.route('/display_page', methods=['GET'])
    def display_page():
        files = os.listdir(app.config['UPLOAD_FOLDER'])
        #files = os.listdir(app.config['LOCAL_THUMBNAILS'])
        print("all the files are")
        print(files)
        '''function to return the HTML page to display the images'''
        flask.session['count'] = 0
        _files = files
        current_file  = os.path.join(app.config['UPLOAD_FOLDER'], _files[0])
        return flask.render_template('photo_display.html', photo=current_file)

    @app.route('/display_page_2', methods=['GET'])
    def display_page_2():
        thumbnail_dir = os.path.join(app.config['UPLOAD_FOLDER'],'thumbnails')
        files = os.listdir(thumbnail_dir)
        print("all the files are")
        print(files)
        '''function to return the HTML page to display the images'''
        flask.session['count'] = 0
        _files = files
        current_file = os.path.join(app.config['UPLOAD_FOLDER'],'thumbnails', _files[0])
        return flask.render_template('photo_display_2.html', photo=current_file)

    @app.route('/get_slider_value', methods=['GET'])
    def get_slider_value():
        _slider_value = int(flask.request.args['value'])
        possible_values = five_item_list
        current_item = five_item_list[_slider_value]
        print(current_item, 'current item')
        return flask.jsonify({'value': _slider_value, 'item':current_item})

    @app.route('/get_photo', methods=['GET'])
    def get_photo():
        files = os.listdir(app.config['UPLOAD_FOLDER'])
        _direction = flask.request.args.get('direction')
        flask.session['count'] = flask.session['count'] + (1 if _direction == 'f' else - 1)
        _files = files
        current_file = os.path.join(app.config['UPLOAD_FOLDER'], _files[flask.session['count']])
        current_filename = _files[flask.session['count']]

        return flask.jsonify(
            {'photo': current_file,'file_name':current_filename, 'forward': str(flask.session['count'] + 1 < len(_files)),
             'back': str(bool(flask.session['count']))})


    @app.route('/get_thumbnail', methods=['GET'])
    def get_thumbnail():
        files = os.listdir(LOCAL_THUMBNAIL_DIRECTORY)
        _direction = flask.request.args.get('direction')
        flask.session['count'] = flask.session['count'] + (1 if _direction == 'f' else - 1)
        _files = files
        current_file = os.path.join(app.config['LOCAL_THUMBNAILS'],  _files[flask.session['count']])

        print(current_file, 'is the current file and the count is ',flask.session['count'])
        current_filename = _files[flask.session['count']]
        print(current_filename)
        return flask.jsonify(
            {'photo': current_file, 'file_name':current_filename, 'forward': str(flask.session['count'] + 1 < len(_files)),
             'back': str(bool(flask.session['count']))})

    return app

def main():

    apiIP = os.getenv('FULLFIELD_PREVIEW_API_IP', "0.0.0.0")
    apiPort = os.getenv('FULLFIELD_PREVIEW_API_PORT', "5454")
    app = create_app()
    app.secret_key = os.urandom(24)
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
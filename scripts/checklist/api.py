from flask import Flask, render_template, send_file, request, url_for, redirect, make_response
import pandas as pd
import tablib
import os
import json
import filecounter


def create_app(test_config=None):

    path_to_flir_csv = os.getenv("FLIR_IR_CAMERA_CSV", 'flirIrCamera_PipelineWatch.csv')
    path_to_stereotop_csv = os.getenv("STEREOTOP_CSV", 'stereoTop_PipelineWatch.csv')
    pipeline_location = os.getenv("PATH_TO_PIPELINE",'')

    pipeline_csv = pipeline_location+"{}_PipelineWatch.csv"

    sensor_names = filecounter.get_sensor_names()

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
    return app

app = create_app()
app.run()


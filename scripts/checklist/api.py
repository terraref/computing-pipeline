from flask import Flask, render_template
import pandas as pd
import tablib
import os
import json




def create_app(test_config=None):

    path_to_flir_csv = os.getenv("FLIR_IR_CAMERA_CSV", 'flirIrCamera_PipelineWatch.csv')
    path_to_stereotop_csv = os.getenv("STEREOTOP_CSV", 'stereoTop_PipelineWatch.csv')

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    dataset = tablib.Dataset()
    # with open('CHECK_TABLE.csv') as f:
    #     dataset.csv = f.read()

    df_flirIr = pd.read_csv(path_to_flir_csv)
    df_stereoTop = pd.read_csv(path_to_stereotop_csv)

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

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World! Saluton, Mondo!'
        # displays json string

    @app.route('/showcsv/<sensor_name>/<showall>')
    def showcsv(sensor_name, showall):

        # data = dataset.html

        if showall.lower() == 'true':
            if sensor_name.lower() == 'stereotop':
                return df_stereoTop.to_html()
            elif sensor_name.lower() == 'flirircamera':
                return df_flirIr.to_html()
        else:
            if sensor_name.lower() == 'stereotop':
                return df_stereoTop.tail(14).to_html()
            elif sensor_name.lower() == 'flirircamera':
                return df_flirIr.tail(14).to_html()

    return app



app = create_app()
app.run()
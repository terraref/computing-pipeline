import pandas as pd
import numpy as np


class SensorStatFormatter:

    def __init__(self, path_to_csv):
        self.df = pd.read_csv(path_to_csv, index_col=False)

    def apply_styling(self):
        return self.df.columns.values

a = 'stereoTop.csv'

new_sensor = SensorStatFormatter(a)

v = new_sensor.apply_styling()
print(v)
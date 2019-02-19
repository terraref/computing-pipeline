from terrautils.betydb import get_experiments
import os
import json

os.environ['BETYDB_KEY'] = 'JGF9SNjh94d0JhamcLauR83RLSqP6OGm2CMOdRZA'
experiment_json = 'experiments.json'

exp = get_experiments()
print(type(exp[0]))
print(len(exp))
with open(experiment_json, 'w+') as fout:
    json.dump(exp, fout)
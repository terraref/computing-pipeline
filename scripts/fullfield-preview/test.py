from terrautils.betydb import get_experiments, get_sites, get_site, get_cultivars
import os
import json

os.environ['BETYDB_KEY'] = 'JGF9SNjh94d0JhamcLauR83RLSqP6OGm2CMOdRZA'
experiment_json = 'experiments.json'
cultivars_json = 'cultivars.json'
sites_json = 'sites.json'
site_json = 'site.json'

print("doing the sript")

cultivars = get_cultivars()
print('got cultivars')
with open(cultivars_json, 'w+') as fout:
     json.dump(cultivars, fout)
exp = get_experiments(associations_mode='full_info')
# sites = get_sites(filter_date='2018-06-01',city="Maricopa")
# print('did we get sites?')
# print(type(sites))
# print(type(exp[0]))
# print(len(exp))
with open(experiment_json, 'w+') as fout:
    json.dump(exp, fout)

# with open(sites_json, 'w+') as fout:
#     json.dump(sites, fout)

test_sites_id = '6000014549'
test_sites_sitename = 'MAC Field Scanner Season 6 Range 4 Column 6'

site = get_site(test_sites_id)
print(site)
with open(site_json, 'w+') as fout:
     json.dump(site, fout)
print('done')
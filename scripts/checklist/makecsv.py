import csv
import datetime

path_to_cvs = '/Users/helium/Desktop/flirIrCamera_PipelineWatch.csv'

f = open(path_to_cvs, 'r')

reader = csv.reader(f)

lines = []
for row in reader:
    lines.append(row)

f.close()

def generate_dates_in_range(start_date_string):
    start_date = datetime.datetime.strptime(start_date_string, '%Y-%m-%d')
    date_strings = []
    for i in range(0, 29):
        current_date = start_date + datetime.timedelta(days=i)
        current_date_string = current_date.strftime('%Y-%m-%d')
        date_strings.append(current_date_string)
    return date_strings

dates_in_range = generate_dates_in_range('2018-07-01')

new_row = lines[-1]

new_rows = []

for each in dates_in_range:
    current_row = new_row[:]
    current_row[0] = each
    new_rows.append(current_row)

with open('/Users/helium/Desktop/flirIrCamera_PipelineWatch2.csv','w') as f:
    writer=csv.writer(f)
    writer.writerow(lines[0])
    for r in new_rows:
        writer.writerow(r)

print('done')
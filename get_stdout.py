import os
import csv
from time import sleep
import subprocess
import datetime

f = open('./stdout/r.csv', 'w', newline = '')
writer = csv.writer(f)
result = subprocess.run('docker logs ecstream-taskmanager-1', capture_output = True, text = True, shell = True)
result = result.stdout
result = result.split('\n')
'''
for row in result:
    if len(row) < 1 or row[1] != '>':
        continue
    if row[1] == '>':
        row = row[4:-1]
    else:
        row = row[5:-1]
    #row = row[1:-1]
    row = row.split(', ')
    writer.writerow(row)
'''
for row in result:
    if len(row) <= 1 or row[1] != '>':
        continue
    if row[1] == '>':
        row = row[4:-1]
    else:
        row = row[5:-1]
    #row = row[1:-1]
    row = row.split(', ')
    writer.writerow(row)

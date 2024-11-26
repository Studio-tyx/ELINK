import os
import csv
from time import sleep
import subprocess
import datetime

f = open('./result/test.csv', 'w', newline = '')
writer = csv.writer(f)
head = ['time', 'kafka cpu', 'kafka memory','kibana cpu', 'kibana memory', 'elasticsearch cpu', 'elasticsearch memory', 'zookeeper cpu', 'zookeeper memory', 'jobmanager cpu', 'jobmanager memory', 'taskmanager cpu', 'taskmanager memory']
writer.writerow(head)
f.flush()
while True:
    result = subprocess.run('docker stats --no-stream ecstream-kafka-1 ecstream-kibana-1 ecstream-elasticsearch-1 ecstream-zookeeper-1 ecstream-jobmanager-1 ecstream-taskmanager-1', capture_output = True, text = True, shell = True)
    std = result.stdout
    std = std.split('\n')
    kafka = std[1].split()
    kibana = std[2].split()
    elastic = std[3].split()
    zookeeper = std[4].split()
    job = std[5].split()
    task = std[6].split()
    t = datetime.datetime.now()
    data = [t, kafka[2], kafka[3], kibana[2], kibana[3], elastic[2], elastic[3], zookeeper[2], zookeeper[3], job[2], job[3], task[2], task[3]]
    writer.writerow(data)
    f.flush()

import os
import csv
from time import sleep
import subprocess
import datetime

job_num = 4
job_code = ['job_mean', 'job_mean_sqrt', 'job_min', 'job_max']
for i in range(job_num):
    subprocess.run('docker compose exec jobmanager ./bin/flink run -py /opt/pyflink-walkthrough/workdir/{}.py -d'.format(job_code[i]), shell = True)


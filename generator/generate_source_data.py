################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import random
import csv
import time, calendar
from random import randint
from kafka import KafkaProducer
from kafka import errors 
from json import dumps
from time import sleep
import time

def ceil(i):
    if int(i) >= i:
        return int(i)
    else:
        return int(i) + 1
        
def floor(i):
    if i >= int(i):
        return int(i)
    else:
        return int(i) - 1
        
#producer_cloud = KafkaProducer(bootstrap_servers=['10.162.198.199:9092'],
 #               value_serializer=lambda x: dumps(x).encode('utf-8'))
                
producer_edge = KafkaProducer(bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'))  
                           
operator_num = 4
operators = ['mean', 'mean_sqrt', 'min', 'max']
operator_gamma = [0, 0, 0, 0]

sensor_gamma = max(operator_gamma)
edge_gamma = 1 - min(operator_gamma)

while True:
    HPT_channel_num = 100
    HPT_val = []
    for i in range(0, HPT_channel_num):
        HPT_val.append(round(random.uniform(4126.078,4130.913), 3))
    
    # send data to topics of each operator respectively
    t = time.time()
    
    # send data to cloud
    sensor_name = 'HPT_'
    base = floor(HPT_channel_num * 0.1)
    sep = base * 10
    send_to_cloud_num = HPT_channel_num
    if sensor_gamma != 1:
        send_to_cloud_num = int(base * sensor_gamma * 10)
    for i in range(send_to_cloud_num):
        if i < sep:
            topic_index = int(i / base) + 1
        else:
            topic_index = int(i / base)
        producer_cloud.send(sensor_name + str(topic_index), value = {"timestamp": t, "channel": i + 1, "id": 1, "data": HPT_val[i]}) 
    # data filter at the edge, not included as transmitted data   
    edge_start_index = int(base * (1 - edge_gamma) * 10)
    for i in range(edge_start_index, HPT_channel_num):
        if i < sep:
            topic_index = int(i / base) + 1
        else:
            topic_index = int(i / base)
        producer_edge.send(sensor_name + str(topic_index), value = {"timestamp": t, "channel": i + 1, "id": 1, "data": HPT_val[i]})
            
    sleep(0.1)



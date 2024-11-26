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

                
producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8'))  

key = 0

while True:
    t = time.time()
    for i in range(1):
        producer.send('TIMER', value = {"MSGID": key, "timestamp": t})
        key += 1
        if key >= 100:
             key = 0
    sleep(0.1)



from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Time, Configuration
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, KafkaSource, DeserializationSchema, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.util.java_utils import get_j_env_configuration
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
import threading
import time
import sys
import random

# exceptions happen when loading model from other file, we need to define models here
# **************************************************************************************************************************

# properties

DEFAULT_KEY = 'D'

idField = 'taxi_identifier'

metaFields = ['pickup_datetime', 'timestamp', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude',\
              'dropoff_latitude', 'payment_type']

observableFields = ['taxi_identifier', 'hack_license', 'pickup_datetime', 'timestamp', 'trip_time_in_secs',\
                    'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',\
                    'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount']

observableFields_with_annotation = ['taxi_identifier', 'hack_license', 'pickup_datetime', 'timestamp', 'trip_time_in_secs',\
                    'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',\
                    'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount',\
                    'taxi_company', 'drivername', 'taxi_city']

observableFields_exclude = []
for i in observableFields:
    if i not in metaFields:
        observableFields_exclude.append(i)

range_filter_valid_range = 'trip_time_in_secs:140:3155,trip_distance:1.37:29.86,fare_amount:6.00:201.00,tip_amount:0.65:38.55,tolls_amount:2.50:18.00'
range_vals = range_filter_valid_range.split(',')

class minMax:
    this_min = -1
    this_max = -1
    def __init__(self, _min, _max):
        self.this_min = float(_min)
        self.this_max = float(_max)

useMsgFields = []
rangeMap = {}
for i in range_vals:
    temp = i.split(':')
    useMsgFields.append(temp[0])
    mm = minMax(temp[1], temp[2])
    rangeMap.update({temp[0]: mm})

bloomFilterUseMsgField = 1

interpolationCountWindowSize = 5

interpolationUseMsgFields = ['trip_time_in_secs', 'trip_distance']

joinMaxCount = 17

joinMetaFields = metaFields

joinSchemaFieldOrderList = observableFields_exclude

annotationMap_file = '../resources/taxi-metadata-fulldataset.txt'

annotationUseMsgField = 0 # texi_identifier

schemaColumn = observableFields_with_annotation
schemaUnit = ['string', 'string', 'timestamp', 'time', 'second', 'meter', 'lon', 'lat', 'lon', 'lat', 'string',\
              'dollar', 'percentage', 'percentage', 'dollar', 'dollar', 'dollar', 'string', 'string', 'string']
schemaType = ['sv', 'sv', 'sv', 'sv', 'v', 'v', 'sv', 'sv', 'sv',\
              'sv', 'sv', 'v', 'v', 'v', 'v', 'v', 'v', 'sv', 'sv', 'sv']

schemaUseMsgField = 0

# SenMLParse

class SenMLParse:
    def __init__(self):
        pass
    def senmlparse(self, _input):
        msgId = _input[0]
        msg = str(_input[1])
        trans_time = _input[2]
        start = _input[3]
        hash_map = {DEFAULT_KEY: msg}
        # result is a hash_map, or a dict in Python 
        result = self.doTask(hash_map)

        meta = ''
        for i in range(0, len(metaFields)):
            meta += str(result[metaFields[i]])
            meta += ','
        meta = meta[:-1]
        r = []
        for j in range(0, len(observableFields_exclude)):
            v = (msgId, result[idField], meta, observableFields_exclude[j], result[observableFields_exclude[j]], trans_time, start)
            r.append(v)
        return r
    
    def doTask(self, hash_map):
        m = hash_map[DEFAULT_KEY]
        m = eval(m)
        basetime = m['bt']
        # event should be a list of dict
        event = (m['e'])
        result_map = {}
        result_map.update({'timestamp': basetime})
        for j in range(0, len(event)):
            d = event[j]
            n = d['n']
            v = ''
            if 'v' in d:
                v = d['v']
            else:
                v = d['sv']
            result_map.update({n: v})
        return result_map
        
# RangeFilter
#from properties_ETL import DEFAULT_KEY, useMsgFields, rangeMap

class RangeFilter:
    def __init__(self):
        pass
    def test(i):
        return i
# input field: ('MSGID', 'SENSORID', 'META', 'OBSTYPE', 'OBSVAL')
    def filter(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        trans_time = _input[5]
        start = _input[6]

        hash_map = {obsType: obsVal}
        result = self.doTask(hash_map)
        if not result:
            obsVal = 'null'
        return (msgId, sensorId, meta, obsType, obsVal, trans_time, start)
    
    def doTask(self, hash_map):
        for msgField in useMsgFields:
            if msgField in hash_map:
                obsType = msgField
                obsVal = float(hash_map[msgField])
                mm = rangeMap[msgField]
                if obsVal < mm.this_min or obsVal > mm.this_max:
                    return False
        return True
        
# BloomFilter
from bloom_filter import BloomFilter

class bf:
    def __init__(self):
        self.bloom_filter = BloomFilter(max_elements=10000, error_rate=0.1)

    def filter(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        trans_time = _input[5]
        start = _input[6]
        # BF = BloomFilter(max_elements=10000, error_rate=0.1)
        #global BF
        useMsgField = bloomFilterUseMsgField
        if useMsgField > 0:
            m = obsVal.split(',')[useMsgField - 1]
            contain = (m in self.bloom_filter)
            if contain:
                return (msgId, sensorId, meta, obsType, 'null', trans_time, start)
            else:
                self.bloom_filter.add(m)
                return (msgId, sensorId, meta, obsType, obsVal, trans_time, start)
                
# Interpolation

class Interpolation:
    def __init__(self):
        self.valuesMap = {}

# input field: ('MSGID', 'SENSORID', 'META', 'OBSTYPE', 'OBSVAL')
    def interpolate(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        trans_time = _input[5]
        start = _input[6]

        hash_map = {}
        hash_map.update({obsType: obsVal})
        res = self.doTask(sensorId, obsType, hash_map)
        return (msgId, sensorId, meta, obsType, res, trans_time, start)
    
    def doTask(self, sensorId, obsType, hash_map):
        sensorId = sensorId
        obsType = obsType
        _sum = 0.0

        if interpolationCountWindowSize ==0 or sensorId == 'null' or len(hash_map) ==0:
            return 'null'

        if obsType in interpolationUseMsgFields:
            currentVal = hash_map[obsType]
            key = sensorId + obsType
            if key in self.valuesMap:
                values = self.valuesMap[key]
                if currentVal == 'null':
                    for i in range(0, len(values)):
                        _sum += float(values[i])
                    return _sum / len(values)
                else:
                    if len(values) == interpolationCountWindowSize:
                        values = values[1:]
                    values.append(float(currentVal))
                    self.valuesMap.update({key: values})
                    return float(currentVal)
            elif currentVal != 'null':
                l = []
                l.append(float(currentVal))
                self.valuesMap.update({key: l})
                return float(currentVal)
        return hash_map[obsType]
        
# Join

class Join:
    def __init__(self):
        self.msgIdCountMap = {}

    def join(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        trans_time = _input[5]
        start = _input[6]
        hash_map = {}
        if msgId in self.msgIdCountMap:
            hash_map = self.msgIdCountMap[msgId]
            hash_map.update({obsType: obsVal})
            self.msgIdCountMap.update({msgId: hash_map})
            if len(hash_map) == joinMaxCount:
                joinedValues = ''
                for s in joinSchemaFieldOrderList:
                    joinedValues += str(hash_map[s])
                    joinedValues += ','
                joinedValues = joinedValues[:-1]
                self.msgIdCountMap.pop(msgId)
                return (msgId, meta, 'joinedValue', str(joinedValues), trans_time, start)
        else:
            hash_map.update({obsType: obsVal})
            metaVal = meta.split(',')
            for i in range(0, len(metaVal)):
                hash_map.update({joinMetaFields[i]: metaVal[i]})
            self.msgIdCountMap.update({msgId: hash_map})
            if len(hash_map) == joinMaxCount:
                joinedValues = ''
                for s in joinSchemaFieldOrderList:
                    joinedValues += hash_map[s]
                    joinedValues += ','
                joinedValues = joinedValues[:-1]
                self.msgIdCountMap.pop(msgId)
                return (msgId, meta, 'joinedValue', str(joinedValues), trans_time, start)
                
# Annotation

from AnnotationBolt import a

class annotation:
    annotationMap = {}
    def __init__(self):
        for i in a:
            s = i.split(':')
            self.annotationMap.update({s[0]: s[1]})
        
    def annotate(self, _input):
        msgId = _input[0]
        meta = _input[1]
        obsType = _input[2]
        obsVal = _input[3]
        trans_time = _input[4]
        start = _input[5]

        hash_map = {}
        hash_map.update({DEFAULT_KEY: obsVal})
        res = self.doTask(hash_map)
        return (msgId, meta, 'annotatedValue', res, trans_time, start)
    
    def doTask(self, hash_map):
        _in = hash_map[DEFAULT_KEY]
        annotateKey = _in.split(',')[annotationUseMsgField]
        if annotateKey in self.annotationMap:
            annotation = self.annotationMap[annotateKey]
            anotatedValue = _in + ',' + annotation
            return anotatedValue
        return _in
        
# CsvToSenML
import json

class csvToSenML:
    schemaMap = {}
    timestampField = 0

    def __init__(self):
        for i in range(0, 10):
            if schemaColumn[i] == 'timestamp':
                self.timestampField = i
            s = schemaColumn[i] + ',' + schemaUnit[i] + ',' + schemaType[i]
            self.schemaMap.update({i: s})

    def CTSM(self, _input):
        msgId = _input[0]
        meta = _input[1]
        obsType = _input[2]
        obsVal = _input[3]
        trans_time = _input[4]
        start = _input[5]

        hash_map = {}
        hash_map.update({DEFAULT_KEY: obsVal})
        res = self.doTask(hash_map)
        return (msgId, meta, 'senml', res, trans_time, start)
    
    def doTask(self, hash_map):
        m = hash_map[DEFAULT_KEY]
        val = m.split(',')
        jsonArr = []
        finalSenML = {}
        finalSenML.update({'bt': val[self.timestampField]})
        for i in range(0, len(self.schemaMap)):
            sch = str(self.schemaMap[i]).split(',')
            if i != self.timestampField:
                obj = {}
                obj.update({'n': sch[0]})
                obj.update({sch[2]: val[i]})
                obj.update({'u': sch[1]})
                obj = json.dumps(obj)
                jsonArr.append(obj)
        jsonArr = json.dumps(jsonArr)
        finalSenML.update({'e': jsonArr})
        finalSenML = json.dumps(finalSenML)
        return finalSenML
        
# MQTTPublish

import random
import time

from paho.mqtt import client as mqtt_client

class MQTTPublish:
    def __init__(self):
        self.broker = '10.192.232.252'
        self.port = 1883
        self.topic = "/python/mqtt"
        self.client_id = f'python-mqtt-{random.randint(0, 1000)}'

    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            pass
            '''
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
            '''

        client = mqtt_client.Client(self.client_id)
        client.on_connect = on_connect
        client.connect(self.broker, self.port)
        return client

    def publish(self, _input):
        msgId = _input[0]
        meta = _input[1]
        obsType = _input[2]
        obsVal = _input[3]
        trans_time = _input[4]
        start = _input[5]

        client = self.connect_mqtt()
        result = client.publish(self.topic, bytearray(obsVal.encode('utf-8')))
        client.disconnect()
        '''
        status = result[0]
        if status == 0:
            print(f"Send msg to topic `{self.topic}`")
        else:
            print(f"Failed to send message to topic {self.topic}")
        '''
        t = time.time()
        return (msgId, meta, obsType, obsVal, trans_time, start)

def map_func(i):
    start = time.time()
    i = eval(i)
    return (i['MSGID'], i['PAYLOAD'], start - i['timestamp'], start)
      
def map_func_cloud(i):
    start = time.time()
    i = eval(i)
    return (i['MSGID'], i['PAYLOAD'], i['timestamp'], start)      
      
from json import dumps  
def map_to_kafka_format(_input):
    msg = {'V1': _input[0], 'V2': _input[1], 'V3': _input[2], 'V4': _input[3], 'V5': _input[4], 'V6': _input[5]}
    return dumps(msg)

# **************************************************************************************************************************


env = StreamExecutionEnvironment.get_execution_environment()
config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
#config.set_integer("python.fn-execution.bundle.size", 50)
#config.set_integer("python.fn-execution.arrow.batch.size", 200)
config.set_integer("python.fn-execution.bundle.time", 1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
#env.set_buffer_timeout(1)
env.set_parallelism(4)

# ETL topology: SenMLParse -> RangeFilter -> BloomFilter -> Interpolation -> Join -> Annotation -> CsvToSenML -> MQTTPublish -> sink
# initializing bolt
_sen_ml_parse = SenMLParse()
_range_filter = RangeFilter()
_bloom_filter = bf()
_interpolation = Interpolation()
_join = Join()
_annotation = annotation()
_csv_to_sen_ml = csvToSenML()
_mqtt_publish = MQTTPublish()

source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('TAXI') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

# case pure edge
'''
ds = ds.map(lambda i : map_func(i)) \
       .rebalance() \
       .flat_map(lambda i : _sen_ml_parse.senmlparse(i)) \
       .map(lambda i : _range_filter.filter(i)) \
       .map(lambda i : _bloom_filter.filter(i)) \
       .map(lambda i : _interpolation.interpolate(i)) \
       .map(lambda i : _join.join(i)) \
       .filter(lambda i : True if i is not None else False) \
       .map(lambda i : _annotation.annotate(i)) \
       .map(lambda i : _csv_to_sen_ml.CTSM(i)) \
       .map(lambda i : (i[4], time.time() - i[5]))
ds.print()
'''

#case edge-cloud
ds = ds.map(lambda i : map_func(i)) \
       .rebalance() \
       .flat_map(lambda i : _sen_ml_parse.senmlparse(i)) \
       .map(lambda i : _range_filter.filter(i)) \
       .map(lambda i : _bloom_filter.filter(i)) \
       .map(map_to_kafka_format, Types.STRING()) \
       .map(lambda i : i.__sizeof__())
 
# rest part on the dloud

sink = KafkaSink.builder() \
    .set_bootstrap_servers('10.162.198.199:9092') \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("TAXI")
            .set_value_serialization_schema(SimpleStringSchema())
            .set_key_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()
#ds.sink_to(sink)
ds.print()
env.execute()

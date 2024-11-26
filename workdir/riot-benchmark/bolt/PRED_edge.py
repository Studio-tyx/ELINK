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

parseMetaFields = ['timestamp', 'longitude', 'latitude']
parseIdField = 'source'
parseObservationFields = ['timestamp', 'source', 'longitude', 'latitude', 'temperature', 'humidity', 'light', 'dust', 'airquality_raw']
parseObservationFields_exclude = ['temperature', 'humidity', 'light', 'dust', 'airquality_raw']

decisionTreeUseMsgField = 1
multiVarLinearRegUseMsgField = 1

averageCountWindowSize = 5

# SenMLParse

class SenMLParsePRED:
    def __init__(self):
        pass

    def senmlparse(self, _input):
        msgId = _input[0]
        msg = _input[1]
        trans_time = _input[2]
        start = _input[3]
        hash_map = {}
        hash_map.update({DEFAULT_KEY: msg})
        res = self.doTask(hash_map)

        meta = ''
        obsVal = ''
        for i in range(0, len(parseMetaFields)):
            meta = meta + str(res[parseMetaFields[i]]) + ','
        meta = meta[:-1]
        for j in range(0, len(parseObservationFields_exclude)):
            obsVal = obsVal + str(res[parseObservationFields_exclude[j]]) + ','
        obsVal = obsVal[:-1]
        return (msgId, res[parseIdField], str(meta), 'dummyobsType', str(obsVal), 'MSGTYPE', 'DumbType', trans_time, start)
    
    def doTask(self, hash_map):
        m = str(hash_map[DEFAULT_KEY])
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
        
# Decision Tree

from sklearn import tree

class DecisionTree:
    def __init__(self):
        self.DTC = tree.DecisionTreeClassifier()
        self.train_x = [
            [8,53.7,0,411.02,140],
            [7.5,48.8,0,3148.78,11],
            [31.3,51.7,0,53.88,36],
            [11.7,57,721,1591.11,22],
            [35.2,12,713,305.01,20],
        ]
        self.train_y = [[5], [10], [10], [15], [20]]
        # training
        self.DTC.fit(self.train_x, self.train_y)

    def predict(self, _input):
        msgId = _input[0]
        meta = _input[2]
        msgType = _input[5]
        analyticsType = _input[6]
        obsVal = _input[4]
        trans_time = _input[7]
        start = _input[8]
        
        predict_val = 0
        if decisionTreeUseMsgField > 0:
            predict_val = obsVal.split(',')
            predict_val = [predict_val]
        else:
            predict_val = float(obsVal)
            predict_val = [[predict_val]]

        res = self.DTC.predict(predict_val)[0]
        return (msgId, meta, obsVal, str(res), 'DTC', trans_time, start)
        
# Linear Regression

from sklearn import linear_model

class MultiVarLinearReg:
    def __init__(self):
        self.LR = linear_model.LinearRegression()
        self.train_x = [
            [8,53.7,0,411.02,140],
            [7.5,48.8,0,3148.78,11],
            [31.3,51.7,0,53.88,36],
            [11.7,57,721,1591.11,22],
            [35.2,12,713,305.01,20],
        ]
        self.train_y = [[5], [10], [10], [15], [20]]
        self.LR.fit(self.train_x, self.train_y)

    def predict(self, _input):
        meta = _input[2]
        msgType = _input[5]
        analyticsType = _input[6]
        obsVal = _input[4]
        msgId = _input[0]
        trans_time = _input[7]
        start = _input[8]

        predict_val = []
        if multiVarLinearRegUseMsgField > 0:
            val = obsVal.split(',')
            for i in range(0, len(val)):
                predict_val.append(float(val[i]))
            predict_val = [predict_val]
        else:
            predict_val = float(obsVal)
            predict_val = [[predict_val]]
        # LR = linear_model.LinearRegression()
        res = self.LR.predict(predict_val)[0][0]
        return (msgId, meta, obsVal, str(res), 'MLR', trans_time, start)
        
# Average

class sumCount:
    def __init__(self, l, p):
        self._count = l
        self._sum = p

class Average:
    def __init__(self):
        self.avgMap = {}

    def average(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        trans_time = _input[7]
        start = _input[8]
        key = sensorId + obsType
        _sc = []
        if key in self.avgMap:
            vlist = []
            vals = obsVal.split(',')
            for i in range (0, len(vals)):
                vlist.append(float(vals[i]))
            _sc = self.avgMap[key]
            res = self.doTask(key, _sc, vlist)
            if res != 'null':
                return (msgId, meta, obsVal, str(res)[1:-1], 'AVG', trans_time, start)
        else:
            vlist = []
            vals = obsVal.split(',')
            for i in range (0, len(vals)):
                vlist.append(float(vals[i]))
            sc = sumCount(1, vlist)
            self.avgMap.update({key: sc})
            
    def doTask(self, key, sc, vals):
        _count = sc._count + 1
        _sum = []
        for i in range(0, len(vals)):
            _sum.append(float(sc._sum[i]) + float(vals[i]))
        res = 'null'
        if _count >= averageCountWindowSize:
            res = []
            for j in range(0, len(sc._sum)):
                res.append(sc._sum[j] / _count)
            _count = 0
            _sum = [0, 0, 0, 0, 0]
        new_sc = sumCount(_count, _sum)
        self.avgMap.update({key: new_sc})
        return res
        
# Error Estimation

class ErrorEstimation:
    def __init__(self):
        self.linearRegRes = -1
        self.avgRes = 'null'

    def estimation(self, _input):
        msgId = _input[0]
        meta = _input[1]
        obsVal = _input[2]
        val = _input[3]
        analyticsType = _input[4]
        trans_time = _input[5]
        start = _input[6]
        if analyticsType == 'MLR':
            air_quality = obsVal.split(',')[4]
            self.linearRegRes = val
            if self.avgRes != 'null':
                errval = (float(air_quality) - float(self.linearRegRes)) / float(self.avgRes)
                return (msgId, meta, obsVal, analyticsType, errval, trans_time, start)
        elif analyticsType == 'AVG':
            self.avgRes = val.split(',')[4]
            
# MQTT Publish
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
        publish_msg = _input[4]

        client = self.connect_mqtt()
        result = client.publish(self.topic, bytearray(publish_msg.encode('utf-8')))
        client.disconnect()
        '''
        status = result[0]
        if status == 0:
            print(f"Send msg to topic `{self.topic}`")
        else:
            print(f"Failed to send message to topic {self.topic}")
        '''
        return _input

def map_func_edge(i):
    start = time.time()
    return (eval(i)['MSGID'], eval(i)['PAYLOAD'], start - eval(i)['timestamp'], start)     
    
def map_func_cloud(i):
    start = time.time()
    i = eval(i)
    return (i['MSGID'], i['PAYLOAD'], i['timestamp'], start) 
    
from json import dumps  
def map_to_kafka_format(_input):
    start = time.time()
    msg = []
    if len(_input) == 9:
        msg = {'V1': _input[0], 'V2': _input[1], 'V3': _input[2], 'V4': _input[3], 'V5': _input[4], 'V6': _input[5], 'V7': _input[6], 'V8': _input[7], 'V9': _input[8]}
    else:
        msg = {'V1': _input[0], 'V2': _input[1], 'V3': _input[2], 'V4': _input[3], 'V5': _input[4], 'V6': _input[5], 'V7': start}
    return dumps(msg)


# **************************************************************************************************************************


env = StreamExecutionEnvironment.get_execution_environment()
config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
#config.set_integer("python.fn-execution.bundle.size", 200)
#config.set_integer("python.fn-execution.arrow.batch.size", 200)
config.set_integer("python.fn-execution.bundle.time", 1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
#env.set_buffer_timeout(1)
env.set_parallelism(4)

# ETL topology: SenMLParse -> KalmanFilter -> SlingdingLinearReg -> GroupVIZ -> sink
#                          -> Average -> GroupVIZ -> sink
# initializing bolt
_sen_ml_parse = SenMLParsePRED()
_decision_tree = DecisionTree()
_linear_reg = MultiVarLinearReg()
_avg = Average()
_error_estimation = ErrorEstimation()
_mqtt_publish = MQTTPublish()

source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('SYS') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
'''
# case pure edge

# SenMLParse
ds = ds.map(lambda i : map_func_edge(i)) \
       .rebalance() \
       .map(lambda i : _sen_ml_parse.senmlparse(i))
       
# side decision tree
ds1 = ds.map(lambda i : _decision_tree.predict(i)) \
	.map(lambda i : (i[5], time.time() - i[6]))
#ds1.print()

# side linear regression
ds2 = ds.map(lambda i : _linear_reg.predict(i))

# side average
ds3 = ds.map(lambda i : _avg.average(i)) \
	.filter(lambda i : True if i is not None else False)

ds_to_estimating = ds2.union(ds3)
ds_to_estimating = ds_to_estimating.map(lambda i : _error_estimation.estimation(i)) \
				   .filter(lambda i : True if i is not None else False) \
				   .map(lambda i : (i[5], time.time() - i[6]))
ds_final = ds1.union(ds_to_estimating)
ds_final.print()
'''

# case edge-cloud
ds = ds.map(lambda i : map_func_cloud(i)) \
       .rebalance() \
       .map(lambda i : _sen_ml_parse.senmlparse(i)) \
       .map(map_to_kafka_format, Types.STRING()) \
       .map(lambda i : time.time() - eval(i)['V8'])
ds.print()
'''      
ds1 = ds.map(lambda i : _avg.average(i)) \
        .filter(lambda i : True if i is not None else False) \
	.map(map_to_kafka_format, Types.STRING())

ds2 =  ds.map(map_to_kafka_format, Types.STRING()) \
         .filter(lambda i : True if i is not None else False)
''' 
# rest part on the dloud

sink1 = KafkaSink.builder() \
    .set_bootstrap_servers('10.162.198.199:9092') \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("PRED1")
            .set_value_serialization_schema(SimpleStringSchema())
            .set_key_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()
sink2 = KafkaSink.builder() \
    .set_bootstrap_servers('10.162.198.199:9092') \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("PRED2")
            .set_value_serialization_schema(SimpleStringSchema())
            .set_key_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()

#ds.sink_to(sink1) 
#ds2.sink_to(sink2)
env.execute()

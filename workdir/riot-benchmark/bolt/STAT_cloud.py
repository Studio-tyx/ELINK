from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Time, Configuration
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, KafkaSource, DeserializationSchema, KafkaOffsetsInitializer
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

idField = 'source'

metaFields = ['timestamp', 'source', 'longitude', 'latitude']

observableFields = ['timestamp', 'source', 'longitude', 'latitude', 'temperature', \
                    'humidity', 'light', 'dust', 'airquality_raw']

observableFields_exclude = []
for i in observableFields:
    if i not in metaFields:
        observableFields_exclude.append(i)

averageCountWindowSize = 1

averageUseMsgField = 6

averageUseMsgFields = ['temperature', 'humidity', 'light', 'dust', 'airquality_raw']

kalmanFilterUseMsgFields = ['temperature', 'humidity', 'light', 'dust', 'airquality_raw']

kalmanFilterUseMsgField = 1

kalmanFilterProcessNoise = 0.125
kalmanFilterSensorNoise = 0.32
kalmanFilterEstimatedError = 30

slingdingLinearRegUseMsgField = -1
slingdingLinearRegWindowSizeTrain = 10
slingdingLinearRegWindowSizePredict = 10

accObsType = ['SLR']
accTupleWindowSize = 10
accMetaTimestampField = 0

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
        
# KalmanFilter

class KalmanFilter:
    def __init__(self, val):
        self.x0_previousEstimation = val

    def filter(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        trans_time = _input[5]
        start = _input[6]
        q_prosessNoise = kalmanFilterProcessNoise
        r_sensorNoise = kalmanFilterSensorNoise
        p0_priorErrorCovariance = kalmanFilterEstimatedError

        if obsType in kalmanFilterUseMsgFields:
            z_measuredValue = -1
            if kalmanFilterUseMsgField > 0:
                z_measuredValue = float(obsVal.split(',')[kalmanFilterUseMsgField - 1])
            elif kalmanFilterUseMsgField == 0:
                z_measuredValue = float(obsVal)

            p1_currentErrorCovariance = p0_priorErrorCovariance + q_prosessNoise
            k_kalmanGain = p1_currentErrorCovariance / (p1_currentErrorCovariance + r_sensorNoise)
            x1_currentEstimation = self.x0_previousEstimation + k_kalmanGain * (z_measuredValue - self.x0_previousEstimation)
            p1_currentErrorCovariance = (1 - k_kalmanGain) * p1_currentErrorCovariance

            currentEstimation = float(x1_currentEstimation)

            return (msgId, sensorId, meta, obsType, str(currentEstimation), trans_time, start)
            
# SlidingLinearRegression

from sklearn import linear_model 

class SlidingLinearReg:
    def __init__(self):
        self.itemCount = 0
        self.train_x = []
        self.train_y = []
        self.LR = linear_model.LinearRegression()

    def predict(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        kalmanFilterUpdatedVal = _input[4]
        trans_time = _input[5]
        start = _input[6]

        self.itemCount += 1

        item = -1
        if slingdingLinearRegUseMsgField > 0:
            item = float(kalmanFilterUpdatedVal.split(',')[slingdingLinearRegUseMsgField -1])
        else:
            item = float(kalmanFilterUpdatedVal)

        self.train_x.append([self.itemCount])
        self.train_y.append([item])
        predictions = []
        if self.itemCount > slingdingLinearRegWindowSizeTrain:
            self.train_x.pop(0)
            self.train_y.pop(0)
            self.LR.fit(self.train_x, self.train_y)
            for j in range(1, slingdingLinearRegWindowSizePredict + 1):
                predictions.append(self.LR.predict([[self.itemCount + j]]))

            res_str = ''
            for i in range(0, len(predictions)):
                res_str += str(predictions[i][0][0])
                res_str += ','
            res_str = res_str[:-1]
            meta = meta + ',' + obsType
            obsType = 'SLR'
            return (msgId, sensorId, meta, obsType, str(res_str), trans_time, start)
            
# GroupVIZ

from queue import PriorityQueue
import matplotlib.pyplot as plt
import io
import zlib

class GroupVIZ:
    def __init__(self):
        self.counter = 0
        self.valueMap = {}

    def gviz(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        metaVals = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        trans_time = _input[5]
        start = _input[6]
        
        metaVal = metaVals.split(',')
        meta = metaVal[len(metaVal) - 1]
        ts = metaVal[accMetaTimestampField]
        innerHashMap = {}
        queue = PriorityQueue()
        self.counter += 1
        key = str(sensorId) + str(obsType)
        if key not in self.valueMap:
            innerHashMap.update({meta: queue})
            self.valueMap.update({key: innerHashMap})
        else:
            innerHashMap = self.valueMap[str(key)]
        
        if meta in innerHashMap:
            queue = innerHashMap[meta]
        else:
            queue = PriorityQueue()
            innerHashMap.update({meta: queue})

        if obsType in accObsType:
            predVals = obsVal.split(',')
            for i in predVals:
                queue.put((i, ts))
        else:
            queue.put((1, 1))
        
        innerHashMap.update({meta: queue})
        self.valueMap.update({key: innerHashMap})

        res = 0
        if self.counter == accTupleWindowSize:
            res = 1
            last_map = self.valueMap
            self.valueMap = {}
            self.counter = 0
        
        if res == 1:
            for value in last_map.values():
                inputForPlotMap = value
                byte_img = self.plot(inputForPlotMap)
                #byte_compressed = zlib.compress(byte_img)
                return (msgId, 'complete', trans_time, start)
                
            
    def plot(self, _map):
        fig = plt.figure(figsize = (8, 6))
        plt.title('title')
        plt.xlabel('X')
        plt.ylabel('Y')

        x_data = []
        y_data = []
        for key in _map.keys():
            obsType = key
            queue = _map[key]
            while not queue.empty():
                e = queue.get()
                x_data.append(e[1])
                y_data.append(e[0])
            plt.plot(x_data, y_data)

        plt.plot(x_data, y_data)
        #plt.show()
        canvas = fig.canvas
        '''
        buffer = io.BytesIO()
        canvas.print_figure(buffer)
        byte_img = buffer.getvalue()
        buffer.close()
        '''
        plt.close()
        return 1
        
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
        trans_time = _input[5]
        start = _input[6]
        if obsType in averageUseMsgFields:
            key = str(sensorId) + str(obsType)
            _sc = 0
            if key in self.avgMap:
                _sc = self.avgMap[key]
                res = self.doTask(key, _sc, obsVal)
                if res != 'null':
                    return (msgId, sensorId, meta, 'AVG', str(res), trans_time, start)
            else:
                sc = sumCount(1, float(obsVal))
                self.avgMap.update({key: sc})
            
    def doTask(self, key, sc, val):
        _count = sc._count + 1
        _sum = sc._sum + float(val)
        res = 'null'
        if _count >= averageCountWindowSize:
            res = _sum / _count
            _count = 0
            _sum = 0
        new_sc = sumCount(_count, _sum)
        self.avgMap.update({key: new_sc})
        return res

def map_func(i):
    start = time.time()
    return (eval(i)['MSGID'], eval(i)['PAYLOAD'], start - eval(i)['timestamp'], start)      


# **************************************************************************************************************************


env = StreamExecutionEnvironment.get_execution_environment()
config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
#config.set_integer("python.fn-execution.bundle.size", 200)
#config.set_integer("python.fn-execution.arrow.batch.size", 200)
config.set_integer("python.fn-execution.bundle.time", 1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
#env.set_buffer_timeout(1)
env.set_parallelism(8)

# ETL topology: SenMLParse -> KalmanFilter -> SlingdingLinearReg -> GroupVIZ -> sink
#                          -> Average -> GroupVIZ -> sink
# initializing bolt
_sen_ml_parse = SenMLParse()
_kalman_filter = KalmanFilter(5)
_sliding_linear_reg = SlidingLinearReg()
_gviz = GroupVIZ()
_avg = Average()

# case pure cloud
'''
source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('SYS') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

# SenMLParse
ds = ds.map(lambda i : map_func(i)) \
       .rebalance() \
       .flat_map(lambda i : _sen_ml_parse.senmlparse(i)) \
       
# side kalman filter
ds1 = ds.map(lambda i : _kalman_filter.filter(i)) \
       .map(lambda i : _sliding_linear_reg.predict(i)) \
       .filter(lambda i : True if i is not None else False)
       

# side average 
ds2 = ds.map(lambda i : _avg.average(i)) \
       .filter(lambda i : True if i is not None else False)
       
       
ds3 = ds1.union(ds2) \
         .map(lambda i : _gviz.gviz(i)) \
         .filter(lambda i : True if i is not None else False) \
         .map(lambda i : (i[2], time.time() - i[3]))

ds3.print()
'''

# case edge-cloud
source1 = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('STAT1') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()
        
source2 = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('STAT2') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()
ds1 = env.from_source(source1, WatermarkStrategy.no_watermarks(), "Kafka Source")
ds2 = env.from_source(source2, WatermarkStrategy.no_watermarks(), "Kafka Source")

def map_ec(i):
    start = time.time()
    i = eval(i)
    return (i['MSGID'], i['SENSORID'], i['META'], i['OBSTYPE'], i['OBSVAL'], time.time() - i['TRANS'], time.time())
    
ds1 = ds1.map(lambda i : map_ec(i)) \
        .rebalance() \
        .filter(lambda i : True if i is not None else False)  
        
ds2 = ds2.map(lambda i : map_ec(i)) \
         .rebalance()
            
ds = ds1.union(ds2) \
        .map(lambda i : _gviz.gviz(i)) \
        .filter(lambda i : True if i is not None else False) \
        .map(lambda i : (i[2], time.time() - i[3]))

ds.print()

env.execute()
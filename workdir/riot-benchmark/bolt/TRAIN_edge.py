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
import pickle

# exceptions happen when loading model from other file, we need to define models here
# **************************************************************************************************************************

# properties

default_model = {
  "meta": "decision-tree",
  "feature_importances_": [
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.4245297894999867,
    0.0,
    0.0,
    0.39702026297677956,
    0.02368943909521627,
    0.046465929707028834,
    0.10829457872098862
  ],
  "max_features_": 13,
  "n_classes_": 3,
  "n_features_": 13,
  "n_outputs_": 1,
  "tree_": {
    "max_depth": 3,
    "node_count": 11,
    "nodes": [
      [
        1,
        4,
        9,
        3.819999933242798,
        0.6619406867845994,
        124,
        124.0
      ],
      [
        2,
        3,
        11,
        3.694999933242798,
        0.08869659275283936,
        43,
        43.0
      ],
      [
        -1,
        -1,
        -2,
        -2.0,
        0.0,
        41,
        41.0
      ],
      [
        -1,
        -1,
        -2,
        -2.0,
        0.0,
        2,
        2.0
      ],
      [
        5,
        8,
        6,
        1.5800000429153442,
        0.5639384240207286,
        81,
        81.0
      ],
      [
        6,
        7,
        10,
        0.9699999988079071,
        0.054012345679012363,
        36,
        36.0
      ],
      [
        -1,
        -1,
        -2,
        -2.0,
        0.0,
        35,
        35.0
      ],
      [
        -1,
        -1,
        -2,
        -2.0,
        0.0,
        1,
        1.0
      ],
      [
        9,
        10,
        12,
        670.0,
        0.19753086419753085,
        45,
        45.0
      ],
      [
        -1,
        -1,
        -2,
        -2.0,
        0.0,
        5,
        5.0
      ],
      [
        -1,
        -1,
        -2,
        -2.0,
        0.0,
        40,
        40.0
      ]
    ],
    "values": [
      [
        [
          42.0,
          47.0,
          35.0
        ]
      ],
      [
        [
          2.0,
          41.0,
          0.0
        ]
      ],
      [
        [
          0.0,
          41.0,
          0.0
        ]
      ],
      [
        [
          2.0,
          0.0,
          0.0
        ]
      ],
      [
        [
          40.0,
          6.0,
          35.0
        ]
      ],
      [
        [
          0.0,
          1.0,
          35.0
        ]
      ],
      [
        [
          0.0,
          0.0,
          35.0
        ]
      ],
      [
        [
          0.0,
          1.0,
          0.0
        ]
      ],
      [
        [
          40.0,
          5.0,
          0.0
        ]
      ],
      [
        [
          0.0,
          5.0,
          0.0
        ]
      ],
      [
        [
          40.0,
          0.0,
          0.0
        ]
      ]
    ],
    "nodes_dtype": [
      "<i8",
      "<i8",
      "<i8",
      "<f8",
      "<f8",
      "<i8",
      "<f8"
    ]
  },
  "classes_": [
    0,
    1,
    2
  ],
  "params": {
    "ccp_alpha": 0.0,
    "class_weight": 'null',
    "criterion": "gini",
    "max_depth": 5,
    "max_features": 'null',
    "max_leaf_nodes": 'null',
    "min_impurity_decrease": 0.0,
    "min_impurity_split": 'null',
    "min_samples_leaf": 1,
    "min_samples_split": 2,
    "min_weight_fraction_leaf": 0.0,
    "presort": "deprecated",
    "random_state": 0,
    "splitter": "best"
  }
}


# Table Read

def TableRead(_input, filename = 'training_data_random.txt'):
    # _input as a timer
    msgId = _input[0]
    trans_time = _input[1]
    start = _input[2]
    '''
    f = open(filename, 'r')
    line = f.readline()
    traindata_x = []
    traindata_y = []
    while line:
        line = line[:-1].split(',')
        training_x.append(line[:5])
        training_y.append(line[5:])
        line = f.readline()
    for row in reader:
        traindata_x.append(row)
    traindata = (traindata_x, traindata_y)
    f.close()
    '''
    traindata_x = []
    traindata_y = []
    for i in range(500):
        temperature = random.randrange(-10, 40)
        humidity = random.randrange(0, 100)
        light = random.randrange(0, 1000)
        dust = random.randrange(0, 4000)
        air_quality = random.randrange(0, 200)
        label = random.randrange(0, 5)
        traindata_x.append([temperature, humidity, light, dust, air_quality])
        traindata_y.append([label])
    traindata = (traindata_x, traindata_y)
    return (msgId, traindata, trans_time, start)
            
# Linear Regression Training
from sklearn import linear_model
import sklearn_json as skljson

class LinearRegressionTrain:
    def __init__(self):
        self.LR = linear_model.LinearRegression()
        
    def train(self, _input):
        
        # LR = linear_model.LinearRegression()
        msgId = _input[0]
        traindata = _input[1]
        trans_time = _input[2]
        start = _input[3]
        traindata_x = traindata[0]
        traindata_y = traindata[1]
        self.LR.fit(traindata_x, traindata_y)
        filename = 'MLR' + str(msgId) + '.model'
        '''
        skljson.to_json(self.LR, 'MLR')
        model = skljson.from_json('MLR')
        '''
        model = default_model
        '''
        with open(filename, 'wb') as f:
            pickle.dump(self.LR, f)
        '''
        return (msgId, 'MLR', filename, trans_time, start, model)  
        
# Decision Tree Training
from sklearn import tree

class DecisionTreeTrain:
    def __init__(self):
        self.DTC = tree.DecisionTreeClassifier()

    def train(self, _input):
        # DTC = tree.DecisionTreeClassifier()
        msgId = _input[0]
        traindata = _input[1]
        trans_time = _input[2]
        timestamp = _input[3]
        traindata_x = traindata[0]
        traindata_y = traindata[1]
        self.DTC.fit(traindata_x, traindata_y)
        filename = 'DTC' + str(msgId) + '.model'
        '''
        skljson.to_json(self.DTC, "DTC")
        model = skljson.from_json('DTC')
        '''
        model = default_model
        '''
        with open(filename, 'wb') as f:
            pickle.dump(self.DTC, f)
        '''
        return (msgId, 'DTC', filename, trans_time, timestamp, model)        

def ModelWrite(_input):
    msgId = _input[0]
    sig = _input[1]
    filename = _input[2]
    trans_time = _input[3]
    timestamp = _input[4]
    model = _input[5]
    #model = skljson.from_json()
    with open(filename, 'wb') as f:
        pickle.dump(model, f)
    return (msgId, sig, filename, trans_time, timestamp)
    
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
        publish_msg = _input[2]

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
    i = eval(i)
    return (i['MSGID'], start - i['timestamp'], start)     
    
def map_func_cloud(i):
    start = time.time()
    i = eval(i)
    return (i['MSGID'], i['timestamp'], start) 


from json import dumps  
def map_to_kafka_format(_input):
    msg = []
    if len(_input) == 6:
        msg = {'V1': _input[0], 'V2': _input[1], 'V3': _input[2], 'V4': _input[3], 'V5': _input[4], 'V6': _input[5]}
    else:
        msg = {'V1': _input[0], 'V2': _input[1], 'V3': _input[2], 'V4': _input[3]}
    return dumps(msg)

# **************************************************************************************************************************


env = StreamExecutionEnvironment.get_execution_environment()
config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
#config.set_integer("python.fn-execution.bundle.size", 10)
#config.set_integer("python.fn-execution.arrow.batch.size", 200)
config.set_integer("python.fn-execution.bundle.time", 1)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
#env.set_buffer_timeout(1)
env.set_parallelism(4)

# ETL topology: SenMLParse -> KalmanFilter -> SlingdingLinearReg -> GroupVIZ -> sink
#                          -> Average -> GroupVIZ -> sink
# initializing bolt
_linear_reg_train = LinearRegressionTrain()
_decision_tree_train = DecisionTreeTrain()
_mqtt_publish = MQTTPublish()

source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('TIMER') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
ds = ds.map(lambda i : map_func_edge(i)) \
       .rebalance()
ds = ds.map(lambda i : TableRead(i)) \
       .map(map_to_kafka_format, Types.STRING()) \
        .map(lambda i : i.__sizeof__())
ds.print()
# case pure edge
'''
# SenMLParse
ds = ds.map(lambda i : map_func_edge(i)) \
       .rebalance()
ds = ds.map(lambda i : TableRead(i)) 
#training
ds1 = ds.map(lambda i : _linear_reg_train.train(i)) \
        .map(lambda i : ModelWrite(i)) \
        .map(lambda i : (i[3], time.time() - i[4]))
ds1.print()
# side decision tree training
ds2 = ds.map(lambda i : _decision_tree_train.train(i)) \
        .map(lambda i : ModelWrite(i)) \
        .map(lambda i : (i[3], time.time() - i[4]))
ds2.print()
'''

# case edge-cloud
'''
ds = ds.map(lambda i : map_func_cloud(i)) \
       .rebalance()
ds = ds.map(lambda i : TableRead(i)) 
#training
ds1 = ds.map(lambda i : _linear_reg_train.train(i)) \
        .map(map_to_kafka_format, Types.STRING())
# side decision tree training
ds2 = ds.map(map_to_kafka_format, Types.STRING())

# rest part on the dloud

sink1 = KafkaSink.builder() \
    .set_bootstrap_servers('10.162.198.199:9092') \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("TRAIN1")
            .set_value_serialization_schema(SimpleStringSchema())
            .set_key_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()
sink2 = KafkaSink.builder() \
    .set_bootstrap_servers('10.162.198.199:9092') \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("TRAIN2")
            .set_value_serialization_schema(SimpleStringSchema())
            .set_key_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()
    
ds1.sink_to(sink1)
ds2.sink_to(sink2)
'''
env.execute()

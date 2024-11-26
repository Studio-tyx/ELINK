from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Time, Configuration
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, KafkaSource, DeserializationSchema, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.util.java_utils import get_j_env_configuration
from operators_streaming_p import streaming_min, streaming_max, streaming_mean, streaming_mean_sqrt, streaming_first, streaming_last, streaming_range, streaming_std, streaming_var, streaming_coffvar, streaming_speed, streaming_acc, streaming_displacement, streaming_surge, streaming_mean_wind_speed, streaming_mean_wind_angle, streaming_angle_of_wind_deflection
import threading
import time
import sys

from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

'''
def threadFunc(ds : DataStream, window_size):
    ds = streaming_mean(ds, window_size)
    ds.print()
'''

env = StreamExecutionEnvironment.get_execution_environment()

config = Configuration(j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
#config.set_integer("python.fn-execution.bundle.size", 200)
#config.set_integer("python.fn-execution.arrow.batch.size", 200)
config.set_integer("python.fn-execution.bundle.time", 1)
#
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
#env.set_buffer_timeout(1)
env.set_parallelism(1)

gamma = 0
topics = ['HPT_1', 'HPT_2', 'HPT_3', 'HPT_4', 'HPT_5', 'HPT_6', 'HPT_7', 'HPT_8', 'HPT_9', 'HPT_10']

source1 = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('HPT_1', 'HPT_2', 'HPT_3', 'HPT_4', 'HPT_5', 'HPT_6', 'HPT_7', 'HPT_8', 'HPT_9', 'HPT_10') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()
''' 
source2 = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('HPT2') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()
source3 = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('HPT3') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()
'''
'''      
source4 = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('HPT4') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
'''
ds1 = env.from_source(source1, WatermarkStrategy.no_watermarks(), "Kafka Source")
'''
ds2 = env.from_source(source2, WatermarkStrategy.no_watermarks(), "Kafka Source")
ds3 = env.from_source(source3, WatermarkStrategy.no_watermarks(), "Kafka Source")
'''
'''
ds4 = env.from_source(source4, WatermarkStrategy.no_watermarks(), "Kafka Source")

ds5 = env.from_source(source1, WatermarkStrategy.no_watermarks(), "Kafka Source")
ds6 = env.from_source(source2, WatermarkStrategy.no_watermarks(), "Kafka Source")
ds7 = env.from_source(source3, WatermarkStrategy.no_watermarks(), "Kafka Source")
ds8 = env.from_source(source4, WatermarkStrategy.no_watermarks(), "Kafka Source")
'''

ds1 = streaming_mean_sqrt(ds1, 60)
ds1.print()
'''
ds2 = streaming_mean_sqrt(ds2, 60)
ds2.print()
ds3 = streaming_mean_sqrt(ds3, 60)
ds3.print()
'''
'''
ds4 = streaming_mean_sqrt(ds4, 60)
ds4.print()

ds5 = streaming_mean_sqrt(ds5, 60)
ds5.print()
ds6 = streaming_mean_sqrt(ds6, 60)
ds6.print()
ds7 = streaming_mean_sqrt(ds7, 60)
ds7.print()
ds8 = streaming_mean_sqrt(ds8, 60)
ds8.print()
'''

env.execute()

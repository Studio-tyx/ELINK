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
        .set_topics('HPT_9', 'HPT_10') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .build()

ds1 = env.from_source(source1, WatermarkStrategy.no_watermarks(), "Kafka Source")

ds1 = streaming_min(ds1, 60)
ds1.print()


env.execute()

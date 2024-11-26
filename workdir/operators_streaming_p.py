from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Time
from pyflink.datastream.functions import RuntimeContext, FlatMapFunction, MapFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig, ReducingStateDescriptor
import math
import random
import time


# flink environment should be created in advance
# parameters of operators below should be a flink data source and size of the window

# a prototype. The line to specify a window is cut in order to continuously monitor the latency
'''
def streaming_mean_sqrt(ds : DataStream, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['channel'], i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (i[0], i[1] + 1, math.sqrt((i[2] ** 2 * i[1] + j[2] ** 2) / (i[1] + 1))))
'''

# 平均数
def streaming_mean(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start)
    def mean(i, j):
        result = (i[3] * i[2] + j[3]) / (i[2] + 1)
        return (j[0], i[1], i[2] + 1, result, time.time() - j[4])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean(i, j))
        

# 均方根
def streaming_mean_sqrt(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start)
    def mean_sqrt(i, j):
        time_end = time.time()        
        result = math.sqrt((i[3] ** 2 * i[2] + j[3] ** 2) / (i[2] + 1))
        return (j[0], i[1], i[2] + 1, result, time.time() - j[4])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean_sqrt(i, j))

# 最大值
def streaming_max(ds : DataStream, window_size):
    '''
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .max(1)
    '''
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start)
    def max(i, j):
    	time_start = time.time()
    	result = 0
    	if i[3] > j[3]:
    		result = i[3]
    	else:
    		result = j[3]
    	return (j[0], i[1], 1, result, time.time() - j[4]) 
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : max(i, j))

# 最小值
def streaming_min(ds : DataStream, window_size):
    '''
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .min(1)
    '''
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start)
    def min(i, j):
    	time_start = time.time()
    	result = 0
    	if i[3] < j[3]:
    		result = i[3]
    	else:
    		result = j[3]
    	return (j[0], i[1], 1, result, time.time() - j[4])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : min(i, j))

# 首位值
def streaming_first(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start)
    def first(i, j):
        return (j[0], i[1], 1, i[3], time.time() - j[4])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : first(i, j))

#末位值
def streaming_last(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start)
    def last(i, j):
        return (j[0], i[1], 1, j[3], time.time() - j[4])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : last(i, j))

#极差
def streaming_range(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], i['data'], 0, time_start)
    def range_func(i, j):
        new_i1 = i[3]
        new_i2 = i[4]
        new_i3 = i[5]
        if j[3] < i[3]:
            new_i1 = j[3]
        if j[3] > i[4]:
            new_i2 = j[3]
        new_i3 = new_i2 - new_i1
        return (j[0], i[1], 1, new_i1, new_i2, new_i3, time.time() - j[6])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : range_func(i, j))

# 标准差
def streaming_std(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], 0, time_start)
    def std(i, j):
        n = i[2] + 1
        this_avg = (i[3] * i[2] + j[3]) / n
        this_var = math.sqrt((n - 1) / n ** 2 * (j[3] - i[3]) ** 2 + (n - 1) / n * i[4] ** 2)
        return (j[0], i[1], n, this_avg, this_var, time.time() - j[5])

    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : std(i, j))

# 方差
def streaming_var(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], 0, time_start)
    def var(i, j):
        n = i[2] + 1
        this_avg = (i[3] * i[2] + j[3]) / n
        this_var = (n - 1) / n ** 2 * (j[3] - i[3]) ** 2 + (n - 1) / n * i[4]
        return (j[0], i[1], n, this_avg, this_var, time.time() - j[5])

    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : var(i, j))

# coffvar
def streaming_coffvar(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], 0, time_start)
    def coffvar(i, j):
        n = i[2] + 1
        this_avg = (i[3] * i[2] + j[3]) / n
        this_var = math.sqrt((n - 1) / n ** 2 * (j[3] - i[3]) ** 2 + (n - 1) / n * (i[4] * i[3]) ** 2) / this_avg
        return (j[0], i[1], n, this_avg, this_var, time.time() - j[5])

    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : coffvar(i, j))


# 速度
def streaming_speed(ds : DataStream, interval, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], 0, time_start)
    def speed(i, j, interval):
        result = (j[3] - i[3]) / interval
        return (j[0], i[1], 1, j[3], result, time.time() - j[5])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : speed(i, j, interval))

# 加速度 假定输入的数据流是“速度”而非原始数据流
def streaming_acc(ds : DataStream, interval, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], 0, time_start)
    def acc(i, j, interval):
        result = (j[3] - i[3]) / interval
        return (j[0], i[1], 1, j[3], result, time.time() - j[5])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : acc(i, j, interval))

# 累计位移
def streaming_displacement(ds : DataStream, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], 0, time_start)
    def dis(i, j):
        result = i[4] + abs(j[3] - i[3])
        return (j[0], i[1], 1, j[3], result, time.time() - j[5])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : dis(i, j))

# 突变预警
def streaming_surge(ds : DataStream, threshold, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], False, time_start)
    def surge(i, j):
        result = ((abs(j[2] - i[2])) > threshold)
        return (j[0], i[1], 1, i[3], result, time.time() - j[5])
    return ds.map(lambda i : mapping_func(i)) \
        .key_by(lambda i : i[1]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : surge(i, j))

# 平均风速
def streaming_mean_wind_speed(ds1 : DataStream, channel1, ds2 : DataStream, channel2, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start, i['timestamp'])
    def mean(i, j):
        result = (i[3] * i[2] + j[3]) / (i[2] + 1)
        return (j[0], i[1], i[2] + 1, result, j[4], j[5])
    # calculating the average value of original datasource keyed by timestamp
    ds1 = ds1.map(lambda i : mapping_func(i)) \
        .filter(lambda i : True if i[1] == channel1 else False) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean(i, j))

    ds2 = ds2.map(lambda i : mapping_func(i)) \
        .filter(lambda i : True if i[1] == channel2 else False) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean(i, j))
    
    def cul(i, j):
        result = math.sqrt(i[3] ** 2 + j[3] ** 2)
        return (j[0], result, time.time() - i[4])
    # unioning two streams and calculating the mean wind speed of each timestamp
    return ds1.union(ds2) \
       .key_by(lambda i : i[5]) \
       .reduce(lambda i, j : cul(i, j))

# 平均风向
def streaming_mean_wind_angle(ds1 : DataStream, channel1, ds2 : DataStream, channel2, window_size):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start, i['timestamp'])
    def mean(i, j):
        result = (i[3] * i[2] + j[3]) / (i[2] + 1)
        return (j[0], i[1], i[2] + 1, result, j[4], j[5])
    ds1 = ds1.map(lambda i : mapping_func(i)) \
        .filter(lambda i : True if i[1] == channel1 else False) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean(i, j))

    ds2 = ds2.map(lambda i : mapping_func(i)) \
        .filter(lambda i : True if i[1] == channel2 else False) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean(i, j))

    def angle(i, j):
        temp1 = i[3]
        temp2 = j[3]
        result = 0
        if temp1 > 1 or temp1 < -1:
            temp1 = random.uniform(-1, 1)
        if temp2 > 0:
            result =  math.acos(temp1) / math.sqrt(temp1 * temp1 + temp2 * temp2)
        else:
            result =  360 - math.acos(temp1) / math.sqrt(temp1 * temp1 + temp2 * temp2)
        return (j[0], result, time.time() - i[4])

    return ds1.union(ds2) \
       .key_by(lambda i : i[5]) \
       .reduce(lambda i, j : angle(i, j))  

# 风偏角
def streaming_angle_of_wind_deflection(ds1 : DataStream, channel1, ds2 : DataStream, channel2, window_size, bx = 2.11, by = 2.11):
    def mapping_func(i):
        time_start = time.time()
        i = eval(i)
        return (time_start - i['timestamp'], i['channel'], i['id'], i['data'], time_start, i['timestamp'])
    def mean(i, j):
        result = (i[3] * i[2] + j[3]) / (i[2] + 1)
        return (j[0], i[1], i[2] + 1, result, j[4], j[5])
    ds1 = ds1.map(lambda i : mapping_func(i)) \
        .filter(lambda i : True if i[1] == channel1 else False) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean(i, j))

    ds2 = ds2.map(lambda i : mapping_func(i)) \
        .filter(lambda i : True if i[1] == channel2 else False) \
        .key_by(lambda i : i[1]) \
        .reduce(lambda i, j : mean(i, j))

    def deflection(i, j):
        result = (bx * i[3] + by * j[3]) / (math.sqrt(i[3] * i[3] + j[3] * j[3]) * math.sqrt(bx * bx + by * by))
        if result >1 or result < -1:
            result = random.uniform(-1, 1)
        result = math.acos(result)
        return (j[0], result, time.time() - i[4])

    return ds1.union(ds2) \
       .key_by(lambda i : i[5]) \
       .reduce(lambda i, j : deflection(i, j))
         

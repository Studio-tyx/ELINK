from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows, TumblingProcessingTimeWindows
from pyflink.common import Time
import math

# flink environment should be created in advance
# parameters of operators below should be a flink data source and size of the window

# 平均数
def streaming_mean(ds : DataStream, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (i[0] + 1, (i[1] * i[0] + j[1]) / (i[0] + 1)))

# 均方根
def streaming_mean_sqrt(ds : DataStream, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (i[0] + 1, math.sqrt((i[1] ** 2 * i[0] + j[1] ** 2) / (i[0] + 1))))

# 最大值
def streaming_max(ds : DataStream, window_size):
    '''
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .max(1)
    '''
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (1, i[1]) if i[1] > j[1] else (1, j[1]))

# 最小值
def streaming_min(ds : DataStream, window_size):
    '''
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .min(1)
    '''
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (1, i[1]) if i[1] < j[1] else (1, j[1]))

# 首位值
def streaming_first(ds : DataStream, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : i)

#末位值
def streaming_last(ds : DataStream, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'])) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : j)

#极差
def streaming_range(ds : DataStream, window_size):

    def range_func(i, j):
        new_i1 = i[1]
        new_i2 = i[2]
        new_i3 = i[3]
        if j[1] < i[1]:
            new_i1 = j[1]
        if j[1] > i[2]:
            new_i2 = j[1]
        new_i3 = new_i2 - new_i1
        return (1, new_i1, new_i2, new_i3)
        
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], i['data'], 0)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : range_func(i, j))

# 标准差和方差可以用非批处理的方式计算吗？
# std var coffvar correlate median

# 标准差
def streaming_std(ds : DataStream, window_size):
    
    def std(i, j):
        n = i[0] + 1
        this_avg = (i[1] * i[0] + j[1]) / n
        this_var = math.sqrt((n - 1) / n ** 2 * (j[1] - i[1]) ** 2 + (n - 1) / n * i[2] ** 2)
        return (n, this_avg, this_var)
        
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], 0)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : std(i, j))

# 方差
def streaming_var(ds : DataStream, window_size):

    def var(i, j):
        n = i[0] + 1
        this_avg = (i[1] * i[0] + j[1]) / n
        this_var = (n - 1) / n ** 2 * (j[1] - i[1]) ** 2 + (n - 1) / n * i[2]
        return (n, this_avg, this_var)
        
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], 0)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : var(i, j))

# coffvar
def streaming_coffvar(ds : DataStream, window_size):

    def coffvar(i, j):
        n = i[0] + 1
        this_avg = (i[1] * i[0] + j[1]) / n
        this_var = math.sqrt((n - 1) / n ** 2 * (j[1] - i[1]) ** 2 + (n - 1) / n * (i[2] * i[1]) ** 2) / this_avg
        return (n, this_avg, this_var)
        
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], 0)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : coffvar(i, j))


# 速度
def streaming_speed(ds : DataStream, interval, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], 0)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (1, j[1], (j[1] - i[1]) / interval))

# 加速度 假定输入的数据流是“速度”而非原始数据流
def streaming_acc(ds : DataStream, interval, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], 0)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (1, j[1], (j[1] - i[1]) / interval))

# 累计位移
def streaming_displacement(ds : DataStream, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], 0)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (1, j[1], i[2] + abs(j[1] - i[1])))

# 突变预警
def streaming_surge(ds : DataStream, threshold, window_size):
    return ds.map(lambda i : eval(i)) \
        .map(lambda i: (i['id'], i['data'], False)) \
        .key_by(lambda i : i[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size))) \
        .reduce(lambda i, j : (1, i[1], (abs(j[1] - i[1])) > threshold))

# 平均风速... 这些公式涉及到多个通道的数据，考虑使用join来实现

        



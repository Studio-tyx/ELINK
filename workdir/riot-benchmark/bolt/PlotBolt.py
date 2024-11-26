from queue import PriorityQueue
from properties_STAT import accObsType, accMetaTimestampField, accTupleWindowSize
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
        queue = innerHashMap[meta]

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
                byte_compressed = zlib.compress(byte_img)
                return (msgId, 'complete')
            
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
        buffer = io.BytesIO()
        canvas.print_figure(buffer)
        byte_img = buffer.getvalue()
        buffer.close()
        return byte_img




     





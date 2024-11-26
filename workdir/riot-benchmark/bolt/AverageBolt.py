from properties_STAT import DEFAULT_KEY, averageUseMsgField, averageCountWindowSize, averageUseMsgFields

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
        if obsType in averageUseMsgFields:
            key = sensorId + obsType
            _sc = 0
            if key in self.avgMap:
                _sc = self.avgMap[key]
                res = self.doTask(key, _sc, obsVal)
                if res != 'null':
                    return (msgId, sensorId, meta, 'AVG', str(res))
            else:
                sc = sumCount(1, float(obsVal))
                self.avgMap.update({key: sc})
        return 'null'
            
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


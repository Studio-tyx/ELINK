from properties_PRED import averageCountWindowSize

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
                return (msgId, meta,, obsVal, str(res)[1:-1], 'AVG')
        else:
            vlist = []
            vals = obsVal.split(',')
            for i in range (0, len(vals)):
                vlist.append(float(vals[i]))
            sc = sumCount(1, vlist)
            self.avgMap.update({key: sc})
        return 'null'
            
    def doTask(self, key, sc, vals):
        _count = sc._count + 1
        _sum = []
        print(sc._sum)
        print(vals)
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
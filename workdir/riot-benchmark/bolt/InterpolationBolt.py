from properties_ETL import interpolationCountWindowSize, interpolationUseMsgFields

class Interpolation:
    def __init__(self):
        self.valuesMap = {}

# input field: ('MSGID', 'SENSORID', 'META', 'OBSTYPE', 'OBSVAL')
    def interpolate(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]

        hash_map = {}
        hash_map.update({obsType: obsVal})
        res = self.doTask(sensorId, obsType, hash_map)
        return (msgId, sensorId, meta, obsType, res)
    
    def doTask(self, sensorId, obsType, hash_map):
        sensorId = sensorId
        obsType = obsType
        _sum = 0.0

        if interpolationCountWindowSize ==0 or sensorId == 'null' or len(hash_map) ==0:
            return 'null'

        if obsType in interpolationUseMsgFields:
            currentVal = hash_map[obsType]
            key = sensorId + obsType
            if key in self.valuesMap:
                values = self.valuesMap[key]
                if currentVal == 'null':
                    for i in range(0, len(values)):
                        _sum += float(values[i])
                    return _sum / len(values)
                else:
                    if len(values) == interpolationCountWindowSize:
                        values = values[1:]
                    values.append(float(currentVal))
                    self.valuesMap.update({key: values})
                    return float(currentVal)
            elif currentVal != 'null':
                l = []
                l.append(float(currentVal))
                self.valuesMap.update({key: l})
                return float(currentVal)
        return hash_map[obsType]

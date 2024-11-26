import sys
sys.path.append(':')
from properties_ETL import interpolationCountWindowSize, interpolationUseMsgFields

def doTask(sensorId, obsType, hash_map, valuesMap):
    sensorId = sensorId
    obsType = obsType
    _sum = 0.0

    if interpolationCountWindowSize ==0 or sensorId == 'null' or len(hash_map) ==0:
        return 'null'

    if obsType in interpolationUseMsgFields:
        currentVal = hash_map[obsType]
        key = sensorId + obsType
        if key in valuesMap:
            values = valuesMap[key]
            if currentVal == 'null':
                for i in range(0, len(values)):
                    _sum += float(values[i])
                return _sum / len(values)
            else:
                if len(values) == interpolationCountWindowSize:
                    values = values[1:]
                values.append(float(currentVal))
                valuesMap.update({key: values})
                return float(currentVal)
        elif currentVal != 'null':
            l = []
            l.append(float(currentVal))
            valuesMap.update({key: l})
            return float(currentVal)
    return 0

    
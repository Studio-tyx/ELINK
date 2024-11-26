from properties_ETL import DEFAULT_KEY, useMsgFields, rangeMap

class RangeFilter:
    def __init__(self):
        pass

# input field: ('MSGID', 'SENSORID', 'META', 'OBSTYPE', 'OBSVAL')
    def filter(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]

        hash_map = {obsType: obsVal}
        result = self.doTask(hash_map)
        if not result:
            obsVal = 'null'
        return (msgId, sensorId, meta, obsType, obsVal)
    
    def doTask(self, hash_map):
        for msgField in useMsgFields:
            if msgField in hash_map:
                obsType = msgField
                obsVal = float(hash_map[msgField])
                mm = rangeMap[msgField]
                if obsVal < mm.this_min or obsVal > mm.this_max:
                    return False
        return True

from properties_PRED import DEFAULT_KEY, parseMetaFields, parseIdField, parseObservationFields_exclude

class SenMLParsePRED:
    def __init__(self):
        pass

    def senmlparse(self, _input):
        msgId = _input[0]
        msg = _input[1]

        hash_map = {}
        hash_map.update({DEFAULT_KEY: msg})
        res = self.doTask(hash_map)

        meta = ''
        obsVal = ''
        for i in range(0, len(parseMetaFields)):
            meta = meta + str(res[parseMetaFields[i]]) + ','
        meta = meta[:-1]
        for j in range(0, len(parseObservationFields_exclude)):
            obsVal = obsVal + str(res[parseObservationFields_exclude[j]]) + ','
        obsVal = obsVal[:-1]
        return (msgId, res[parseIdField], str(meta), 'dummyobsType', str(obsVal), 'MSGTYPE', 'DumbType')
    
    def doTask(self, hash_map):
        m = str(hash_map[DEFAULT_KEY])
        m = eval(m)
        basetime = m['bt']
        # event should be a list of dict
        event = (m['e'])
        result_map = {}
        result_map.update({'timestamp': basetime})
        for j in range(0, len(event)):
            d = event[j]
            n = d['n']
            v = ''
            if 'v' in d:
                v = d['v']
            else:
                v = d['sv']
            result_map.update({n: v})
        return result_map



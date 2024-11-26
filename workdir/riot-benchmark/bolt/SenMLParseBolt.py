from properties_ETL import DEFAULT_KEY, metaFields, observableFields, observableFields_exclude, idField

class SenMLParse:
    def __init__(self):
        pass
    def senmlparse(self, _input):
        msgId = _input[0]
        msg = str(_input[1])
        hash_map = {DEFAULT_KEY: msg}
        # result is a hash_map, or a dict in Python 
        result = self.doTask(hash_map)

        meta = ''
        for i in range(0, len(metaFields)):
            meta += str(result[metaFields[i]])
            meta += ','
        meta = meta[:-1]
        r = []
        for j in range(0, len(observableFields_exclude)):
            v = (msgId, result[idField], meta, observableFields_exclude[j], result[observableFields_exclude[j]])
            r.append(v)
        return r
    
    def doTask(self, hash_map):
        m = hash_map[DEFAULT_KEY]
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

from properties_ETL import DEFAULT_KEY

def doTask(hash_map):
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
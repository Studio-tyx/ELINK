from properties_ETL import bloomFilterUseMsgField, DEFAULT_KEY
from bloom_filter import BloomFilter

class bf:
    def __init__(self):
        self.bloom_filter = BloomFilter(max_elements=10000, error_rate=0.1)

    def filter(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        # BF = BloomFilter(max_elements=10000, error_rate=0.1)
        #global BF
        useMsgField = bloomFilterUseMsgField
        if useMsgField > 0:
            m = obsVal.split(',')[useMsgField - 1]
            contain = (m in self.bloom_filter)
            if contain:
                return (msgId, sensorId, meta, obsType, 'null')
            else:
                self.bloom_filter.add(m)
                return (msgId, sensorId, meta, obsType, obsVal)
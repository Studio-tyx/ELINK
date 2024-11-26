from properties_ETL import joinMaxCount, joinMetaFields, joinSchemaFieldOrderList

class Join:
    def __init__(self):
        self.msgIdCountMap = {}

    def join(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        obsVal = _input[4]
        hash_map = {}
        if msgId in self.msgIdCountMap:
            hash_map = self.msgIdCountMap[msgId]
            hash_map.update({obsType: obsVal})
            self.msgIdCountMap.update({msgId: hash_map})
            if len(hash_map) == joinMaxCount:
                joinedValues = ''
                for s in joinSchemaFieldOrderList:
                    joinedValues += str(hash_map[s])
                    joinedValues += ','
                joinedValues = joinedValues[:-1]
                self.msgIdCountMap.pop(msgId)
                return (msgId, meta, 'joinedValue', str(joinedValues))
        else:
            hash_map.update({obsType: obsVal})
            metaVal = meta.split(',')
            for i in range(0, len(metaVal)):
                hash_map.update({joinMetaFields[i]: metaVal[i]})
            self.msgIdCountMap.update({msgId: hash_map})
            if len(hash_map) == joinMaxCount:
                joinedValues = ''
                for s in joinSchemaFieldOrderList:
                    joinedValues += hash_map[s]
                    joinedValues += ','
                joinedValues = joinedValues[:-1]
                self.msgIdCountMap.pop(msgId)
                return (msgId, meta, 'joinedValue', str(joinedValues))
                

from properties_ETL import DEFAULT_KEY, schemaColumn, schemaType, schemaUnit, schemaUseMsgField
import json

class csvToSenML:
    schemaMap = {}
    timestampField = 0

    def __init__(self):
        for i in range(0, 10):
            if schemaColumn[i] == 'timestamp':
                self.timestampField = i
            s = schemaColumn[i] + ',' + schemaUnit[i] + ',' + schemaType[i]
            self.schemaMap.update({i: s})

    def CTSM(self, _input):
        msgId = _input[0]
        meta = _input[1]
        obsType = _input[2]
        obsVal = _input[3]

        hash_map = {}
        hash_map.update({DEFAULT_KEY: obsVal})
        res = self.doTask(hash_map)
        return (msgId, meta, 'senml', res)
    
    def doTask(self, hash_map):
        m = hash_map[DEFAULT_KEY]
        val = m.split(',')
        jsonArr = []
        finalSenML = {}
        finalSenML.update({'bt': val[self.timestampField]})
        for i in range(0, len(self.schemaMap)):
            sch = str(self.schemaMap[i]).split(',')
            if i != self.timestampField:
                obj = {}
                obj.update({'n': sch[0]})
                obj.update({sch[2]: val[i]})
                obj.update({'u': sch[1]})
                obj = json.dumps(obj)
                jsonArr.append(obj)
        jsonArr = json.dumps(jsonArr)
        finalSenML.update({'e': jsonArr})
        finalSenML = json.dumps(finalSenML)
        return finalSenML
    

from properties import DEFAULT_KEY, timestampField, schemaMap
import json

def doTask(hash_map):
    m = hash_map[DEFAULT_KEY]
    val = m.split(',')
    jsonArr = []
    finalSenML = {}
    finalSenML.update({'bt': val[timestampField]})
    for i in range(0, len(schemaMap)):
        sch = str(schemaMap[i]).split(',')
        if i != timestampField:
            obj = {}
            obj.update({'n', sch[0]})
            obj.update({sch[2], val[i]})
            obj.update('u', sch[1])
            obj = json.dumps(obj)
            jsonArr.append(obj)
    jsonArr = json.dumps(jsonArr)
    finalSenML.update({'e', jsonArr})
    finalSenML = json.dumps(finalSenML)
    return finalSenML
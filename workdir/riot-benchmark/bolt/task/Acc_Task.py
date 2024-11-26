from properties import Acc_MultiValueObsType, Acc_MetaTimestampField, Acc_TupleWindowSize

def AccTask(hash_map):
    sensorId = hash_map['SENSORID']
    metaValues = hash_map['META']
    obsVal = hash_map['OBSVAL']
    obsType = hash_map['OBSTYPE']

    multiValueObs = Acc_MultiValueObsType
    multiValObs = multiValueObs.split(',')
    tupleWindowSize = Acc_TupleWindowSize
    timestampField = Acc_MetaTimestampField

    global valueMaps

    metaVal = metaValues.split(',')
    meta = metaVal[len(metaVal) - 1]
    ts = metaVal[timestampField]
    

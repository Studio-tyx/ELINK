import sys
sys.path.append('..')
from properties_ETL import DEFAULT_KEY, annotationUseMsgField

def doTask(hash_map):
    _in = hash_map[DEFAULT_KEY]
    annotateKey = _in.split(',')[anotationUseMsgField]
    if annotateKey in annotationMap:
        annotation = annotationMap[annotateKey]
        anotatedValue = _in + ',' + annotation
        return anotatedValue
    return _in
    
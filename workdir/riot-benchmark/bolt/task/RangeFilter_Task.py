import sys
sys.path.append('..')
from properties_ETL import DEFAULT_KEY, useMsgFields, rangeMap
def doTask(hash_map):
    for msgField in useMsgFields:
        if msgField in hash_map:
            obsType = msgField
            obsVal = float(hash_map[msgField])
            mm = rangeMap[msgField]
            if obsVal < mm.this_min or obsVal > mm.this_max:
                return False
    return True
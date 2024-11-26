
class ErrorEstimation:
    def __init__(self):
        self.linearRegRes = -1
        self.avgRes = 'null'

    def estimation(self, _input):
        msgId = _input[0]
        meta = _input[1]
        obsVal = _input[2]
        val = _input[3]
        analyticsType = _input[4]

        if analyticsType == 'MLR':
            air_quality = obsVal.split(',')[4]
            self.linearRegRes = val
            if self.avgRes != 'null':
                errval = (float(air_quality) - float(self.linearRegRes)) / float(self.avgRes)
                return (msgId, meta, obsVal, analyticsType, errval)
        elif analyticsType == 'AVG':
            self.avgRes = val.split(',')[4]

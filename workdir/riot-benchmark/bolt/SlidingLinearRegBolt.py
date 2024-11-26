from properties_STAT import slingdingLinearRegUseMsgField, slingdingLinearRegWindowSizeTrain, slingdingLinearRegWindowSizePredict
from sklearn import linear_model 

class SlidingLinearReg:
    def __init__(self):
        self.itemCount = 0
        self.train_x = []
        self.train_y = []
        self.LR = linear_model.LinearRegression()

    def predict(self, _input):
        msgId = _input[0]
        sensorId = _input[1]
        meta = _input[2]
        obsType = _input[3]
        kalmanFilterUpdatedVal = _input[4]

        self.itemCount += 1

        item = -1
        if slingdingLinearRegUseMsgField > 0:
            item = float(kalmanFilterUpdatedVal.split(',')[slingdingLinearRegUseMsgField -1])
        else:
            item = float(kalmanFilterUpdatedVal)

        self.train_x.append([self.itemCount])
        self.train_y.append([item])
        predictions = []
        if self.itemCount > slingdingLinearRegWindowSizeTrain:
            self.train_x.pop(0)
            self.train_y.pop(0)
            self.LR.fit(self.train_x, self.train_y)
            for j in range(1, slingdingLinearRegWindowSizePredict + 1):
                predictions.append(self.LR.predict([[self.itemCount + j]]))

            res_str = ''
            for i in range(0, len(predictions)):
                res_str += str(predictions[i][0][0])
                res_str += ','
            res_str = res_str[:-1]
            meta = meta + ',' + obsType
            obsType = 'SLR'
            return (msgId, sensorId, meta, obsType, str(res_str))

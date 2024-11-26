from sklearn import linear_model

class LinearRegressionTrain:
    def __init__(self):
        self.LR = linear_model.LinearRegression()
        
    def LinearRegTrain(self, _input):
        # LR = linear_model.LinearRegression()
        global LR
        msgId = _input[0]
        traindata = _input[1]
        traindata_x = traindata[0]
        traindata_y = traindata[1]
        self.LR.fit(traindata_x, traindata_y)
        filename = 'MLR' + str(msgId) + '.model'
        return (msgId, bytearray(LR), 'MLR', filename)
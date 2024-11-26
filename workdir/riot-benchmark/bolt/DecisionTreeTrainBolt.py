from sklearn import tree

class DecisionTreeTrain:
    def __init__(self):
        self.DTC = tree.DecisionTreeClassifier()

    def DecisionTreeTrain(self, _input):
        # DTC = tree.DecisionTreeClassifier()
        global DTC
        msgId = _input[0]
        traindata = _input[1]
        traindata_x = traindata[0]
        traindata_y = traindata[1]
        self.DTC.fit(traindata_x, traindata_y)
        filename = 'DTC' + str(msgId) + '.model'
        return (msgId, bytearray(DTC), 'DTC', filename)
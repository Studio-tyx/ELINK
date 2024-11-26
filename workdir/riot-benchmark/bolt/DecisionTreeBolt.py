from sklearn import tree
from properties_PRED import decisionTreeUseMsgField

class DecisionTree:
    def __init__(self):
        self.DTC = tree.DecisionTreeClassifier()
        self.train_x = [
            [8,53.7,0,411.02,140],
            [7.5,48.8,0,3148.78,11],
            [31.3,51.7,0,53.88,36],
            [11.7,57,721,1591.11,22],
            [35.2,12,713,305.01,20],
        ]
        self.train_y = [[5], [10], [10], [15], [20]]
        # training
        self.DTC.fit(self.train_x, self.train_y)

    def predict(self, _input):
        msgId = _input[0]
        meta = _input[2]
        msgType = _input[5]
        analyticsType = _input[6]
        obsVal = _input[4]
        
        predict_val = 0
        if decisionTreeUseMsgField > 0:
            predict_val = obsVal.split(',')
            predict_val = [predict_val]
        else:
            predict_val = float(obsVal)
            predict_val = [[predict_val]]

        res = self.DTC.predict(predict_val)[0]
        return (msgId, meta, obsVal, str(res), 'DTC')
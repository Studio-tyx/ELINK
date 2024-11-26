DEFAULT_KEY = 'D'

parseMetaFields = ['timestamp', 'longitude', 'latitude']
parseIdField = 'source'
parseObservationFields = ['timestamp', 'source', 'longitude', 'latitude', 'temperature', 'humidity', 'light', 'dust', 'airquality_raw']
parseObservationFields_exclude = ['temperature', 'humidity', 'light', 'dust', 'airquality_raw']

decisionTreeUseMsgField = 1
multiVarLinearRegUseMsgField = 1

averageCountWindowSize = 5




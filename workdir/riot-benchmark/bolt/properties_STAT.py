DEFAULT_KEY = 'D'

idField = 'source'

metaFields = ['timestamp', 'source', 'longitude', 'latitude']

observableFields = ['timestamp', 'source', 'longitude', 'latitude', 'temperature', \
                    'humidity', 'light', 'dust', 'airquality_raw']

observableFields_exclude = []
for i in observableFields:
    if i not in metaFields:
        observableFields_exclude.append(i)

averageCountWindowSize = 10

averageUseMsgField = 6

averageUseMsgFields = ['temperature', 'humidity', 'light', 'dust', 'airquality_raw']

kalmanFilterUseMsgFields = ['temperature', 'humidity', 'light', 'dust', 'airquality_raw']

kalmanFilterUseMsgField = 1

kalmanFilterProcessNoise = 0.125
kalmanFilterSensorNoise = 0.32
kalmanFilterEstimatedError = 30

slingdingLinearRegUseMsgField = -1
slingdingLinearRegWindowSizeTrain = 10
slingdingLinearRegWindowSizePredict = 10

accObsType = ['SLR']
accTupleWindowSize = 10
accMetaTimestampField = 0



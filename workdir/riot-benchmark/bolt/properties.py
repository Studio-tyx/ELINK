DEFAULT_KEY = 'D'
idField = 'taxi_identifier'
metaFields = ['pickup_datetime', 'timestamp', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude',\
              'dropoff_latitude', 'payment_type']
observableFields = ['taxi_identifier', 'hack_license', 'pickup_datetime', 'timestamp', 'trip_time_in_secs',\
                    'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',\
                    'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount',\
                    'taxi_company', 'drivername', 'taxi_city']
observableFields_exclude = []
for i in observableFields:
    if i not in metaFields:
        observableFields_exclude.append(i)
print(observableFields_exclude)
_min = -1
_max = 50
interpolCountWindowSize = -1
useMsgFields = []
maxCountPossible = -1
schemaFieldOrderList = []
annotationMap = {}
timestampField = -1
schemaMap = {}

bloomFilter_usemsgField = -1

kalmanFilter_usemsgField = -1
kalmanFilter_processNoise = -1
kalmanFilter_sensorNoise = -1
kalmanFilter_estimatedError = -1

SlidingLinearReg_usemsgField = -1
SlidingLinearReg_windowSizeTrain = -1
SlidingLinearReg_windowSizePredict = -1

Acc_MultiValueD = -1
Acc_TupleWindowSize = -1
Acc_MetaTimestampField = -1

SenMLParsePRED_metaFields = []
SenMLParsePRED_observableFields = []

DecisionTree_MSGTYPE = -1
DecisionTree_ANALYTICTYPE = -1
DecisionTree_META = -1

AvgPRED_usemsgFields = []
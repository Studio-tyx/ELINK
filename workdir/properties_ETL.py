DEFAULT_KEY = 'D'

idField = 'taxi_identifier'

metaFields = ['pickup_datetime', 'timestamp', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude',\
              'dropoff_latitude', 'payment_type']

observableFields = ['taxi_identifier', 'hack_license', 'pickup_datetime', 'timestamp', 'trip_time_in_secs',\
                    'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',\
                    'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount']

observableFields_with_annotation = ['taxi_identifier', 'hack_license', 'pickup_datetime', 'timestamp', 'trip_time_in_secs',\
                    'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',\
                    'payment_type', 'fare_amount', 'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount',\
                    'taxi_company', 'drivername', 'taxi_city']

observableFields_exclude = []
for i in observableFields:
    if i not in metaFields:
        observableFields_exclude.append(i)

range_filter_valid_range = 'trip_time_in_secs:140:3155,trip_distance:1.37:29.86,fare_amount:6.00:201.00,tip_amount:0.65:38.55,tolls_amount:2.50:18.00'
range_vals = range_filter_valid_range.split(',')

class minMax:
    this_min = -1
    this_max = -1
    def __init__(self, _min, _max):
        self.this_min = float(_min)
        self.this_max = float(_max)

useMsgFields = []
rangeMap = {}
for i in range_vals:
    temp = i.split(':')
    useMsgFields.append(temp[0])
    mm = minMax(temp[1], temp[2])
    rangeMap.update({temp[0]: mm})

bloomFilterUseMsgField = 1

interpolationCountWindowSize = 5

interpolationUseMsgFields = ['trip_time_in_secs', 'trip_distance']

joinMaxCount = 17

joinMetaFields = metaFields

joinSchemaFieldOrderList = observableFields_exclude

annotationMap_file = '../resources/taxi-metadata-fulldataset.txt'

annotationUseMsgField = 0 # texi_identifier

schemaColumn = observableFields_with_annotation
schemaUnit = ['string', 'string', 'timestamp', 'time', 'second', 'meter', 'lon', 'lat', 'lon', 'lat', 'string',\
              'dollar', 'percentage', 'percentage', 'dollar', 'dollar', 'dollar', 'string', 'string', 'string']
schemaType = ['sv', 'sv', 'sv', 'sv', 'v', 'v', 'sv', 'sv', 'sv',\
              'sv', 'sv', 'v', 'v', 'v', 'v', 'v', 'v', 'sv', 'sv', 'sv']

schemaUseMsgField = 0
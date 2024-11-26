import sys
sys.path.append('..')
from SenMLParseBolt import SenMLParse

test_input = (1358101800000,{"e":[{"u":"string","n":"taxi_identifier","sv":"149298F6D390FA640E80B41ED31199C5"},{"u":"string","n":"hack_license","sv":"08F944E76118632BE09B9D4B04C7012A"},{"u":"time","n":"pickup_datetime","sv":"2013-01-13 23:36:00"},{"v":"1440","u":"second","n":"trip_time_in_secs"},{"v":"9.08","u":"meter","n":"trip_distance"},{"u":"lon","n":"pickup_longitude","sv":"-73.982071"},{"u":"lat","n":"pickup_latitude","sv":"40.769081"},{"u":"lon","n":"dropoff_longitude","sv":"-73.915878"},{"u":"lat","n":"dropoff_latitude","sv":"40.868458"},{"u":"string","n":"payment_type","sv":"CSH"},{"v":"29.00","u":"dollar","n":"fare_amount"},{"v":"0.50","u":"percentage","n":"surcharge"},{"v":"0.50","u":"percentage","n":"mta_tax"},{"v":"0.00","u":"dollar","n":"tip_amount"},{"v":"0.00","u":"dollar","n":"tolls_amount"},{"v":"30.00","u":"dollar","n":"total_amount"}],"bt":1358101800000})
smlp = SenMLParse()
print(smlp.senmlparse(test_input))
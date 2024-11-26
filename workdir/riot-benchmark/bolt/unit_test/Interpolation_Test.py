import sys
sys.path.append('..')
from InterpolationBolt import Interpolation

test_input1 = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'trip_time_in_secs', '1')
test_input2 = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'trip_time_in_secs', '2')
test_input3 = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'trip_time_in_secs', '3')
test_input4 = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'trip_time_in_secs', '4')
test_input5 = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'trip_time_in_secs', 'null')

itp = Interpolation()

itp.interpolate(test_input1)
itp.interpolate(test_input2)
itp.interpolate(test_input3)
itp.interpolate(test_input4)
print(itp.interpolate(test_input5))
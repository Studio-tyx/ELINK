import sys
sys.path.append('..')
from RangeFilterBolt import RangeFilter

test_input1 = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'trip_time_in_secs', '139.5')
test_input2 = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'trip_time_in_secs', '141')

RF = RangeFilter()

print(RF.filter(test_input1))
print(RF.filter(test_input2))
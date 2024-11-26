import sys
sys.path.append('..')
from KalmanFilterBolt import KalmanFilter

test_input = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'temperature', '26')
kf = KalmanFilter(5)
print(kf.filter(test_input))
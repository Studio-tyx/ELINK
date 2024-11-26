import sys
sys.path.append('..')
from ErrorEstimationBolt import ErrorEstimation

test_input1 = (1422748800000, '1422748800000,6.1668213,46.1927629', '8,53.7,0,411.02,140', '4.999999999999995', 'MLR')
test_input2 = (1422748800000, '1422748800000,-122.230081,37.790237', '11.7, 42.239999999999995, 144.2, 1040.958, 41.8', '35.2,12,713,305.01,20', 'AVG')
test_input3 = (1422748800000, '1422748800000,6.1668213,46.1927629', '8,53.7,0,411.02,140', '6', 'MLR')

ee = ErrorEstimation()
print(ee.estimation(test_input1))
print(ee.estimation(test_input2))
print(ee.estimation(test_input3))
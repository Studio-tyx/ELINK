import sys
sys.path.append('..')
from MultiVarLinearRegBolt import MultiVarLinearReg

test_input = (1422748800000, 'ci4lr75sl000802ypo4qrcjda23', '1422748800000,6.1668213,46.1927629', 'dummyobsType', '8,53.7,0,411.02,140', 'MSGTYPE', 'DumbType')

MLR = MultiVarLinearReg()
print(MLR.predict(test_input))
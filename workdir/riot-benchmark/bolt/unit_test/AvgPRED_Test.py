import sys
sys.path.append('..')
from AvgPREDBolt import Average

test_input = []
test_input.append((1422748800000, 'ci4lr75sl000802ypo4qrcjda23', '1422748800000,6.1668213,46.1927629', 'dummyobsType', '8,53.7,0,411.02,140', 'MSGTYPE', 'DumbType'))
test_input.append((1422748800000, 'ci4lr75sl000802ypo4qrcjda23', '1422748800000,6.211192,46.246715', 'dummyobsType', '7.5,48.8,0,3148.78,11', 'MSGTYPE', 'DumbType'))
test_input.append((1422748800000, 'ci4lr75sl000802ypo4qrcjda23', '1422748800000,-43.178667,-22.919665', 'dummyobsType', '31.3,51.7,0,53.88,36', 'MSGTYPE', 'DumbType'))
test_input.append((1422748800000, 'ci4lr75sl000802ypo4qrcjda23', '1422748800000,121.435432,31.226463', 'dummyobsType', '11.7,57,721,1591.11,22', 'MSGTYPE', 'DumbType'))
test_input.append((1422748800000, 'ci4lr75sl000802ypo4qrcjda23', '1422748800000,-122.230081,37.790237', 'dummyobsType', '35.2,12,713,305.01,20', 'MSGTYPE', 'DumbType'))

avg = Average()
for i in range(0, len(test_input)):
    print(avg.average(test_input[i]))
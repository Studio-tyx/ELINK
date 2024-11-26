import sys
sys.path.append('..')
from AnnotationBolt import annotation

a = annotation('../resources/taxi-metadata-fulldataset.txt')

test_input1 = (1358101800000, '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'joinedValue', '11,10,14,12,19,12,12,12,12,13')
test_input2 = (1358101800000, '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', 'joinedValue', 'FFCFA7AFF0DE2B5081C6C1A11099A691,10,14,12,19,12,12,12,12,13')
print(a.annotate(test_input1))
print(a.annotate(test_input2))
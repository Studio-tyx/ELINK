import sys
sys.path.append('..')
from JoinBolt import Join
from properties_ETL import joinMaxCount, joinMetaFields, joinSchemaFieldOrderList
import random

j = Join()
index = 1
for field in joinSchemaFieldOrderList:
    test_input = (1358101800000, '149298F6D390FA640E80B41ED31199C5', '2013-01-13 23:36:00,1358101800000,-73.982071,40.769081,-73.915878,40.868458,CSH', field, random.randrange(10, 20))
    r = j.join(test_input)
    print(str(index) + ':  ' + str(r))
    index += 1


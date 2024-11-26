import sys
sys.path.append('..')
from SenMLParsePREDBolt import SenMLParsePRED

test_input1 = (1422748800000,{"e":[{"u":"string","n":"source","sv":"ci4lr75sl000802ypo4qrcjda23"},{"v":"6.1668213","u":"lon","n":"longitude"},{"v":"46.1927629","u":"lat","n":"latitude"},{"v":"8","u":"far","n":"temperature"},{"v":"53.7","u":"per","n":"humidity"},{"v":"0","u":"per","n":"light"},{"v":"411.02","u":"per","n":"dust"},{"v":"140","u":"per","n":"airquality_raw"}],"bt":1422748800000})
test_input2 = (1422748800000,{"e":[{"u":"string","n":"source","sv":"ci4lr75v6000a02ypa256zigk27"},{"v":"6.211192","u":"lon","n":"longitude"},{"v":"46.246715","u":"lat","n":"latitude"},{"v":"7.5","u":"far","n":"temperature"},{"v":"48.8","u":"per","n":"humidity"},{"v":"0","u":"per","n":"light"},{"v":"3148.78","u":"per","n":"dust"},{"v":"11","u":"per","n":"airquality_raw"}],"bt":1422748800000})
test_input3 = (1422748800000,{"e":[{"u":"string","n":"source","sv":"ci4oethyi000302ymejc2wc2j2"},{"v":"-43.178667","u":"lon","n":"longitude"},{"v":"-22.919665","u":"lat","n":"latitude"},{"v":"31.3","u":"far","n":"temperature"},{"v":"51.7","u":"per","n":"humidity"},{"v":"0","u":"per","n":"light"},{"v":"53.88","u":"per","n":"dust"},{"v":"36","u":"per","n":"airquality_raw"}],"bt":1422748800000})
test_input4 = (1422748800000,{"e":[{"u":"string","n":"source","sv":"ci4s0caqw000002wey2s695ph19"},{"v":"121.435432","u":"lon","n":"longitude"},{"v":"31.226463","u":"lat","n":"latitude"},{"v":"11.7","u":"far","n":"temperature"},{"v":"57","u":"per","n":"humidity"},{"v":"721","u":"per","n":"light"},{"v":"1591.11","u":"per","n":"dust"},{"v":"22","u":"per","n":"airquality_raw"}],"bt":1422748800000})
test_input5 = (1422748800000,{"e":[{"u":"string","n":"source","sv":"ci4usvy81000302s7whpk8qlp0"},{"v":"-122.230081","u":"lon","n":"longitude"},{"v":"37.790237","u":"lat","n":"latitude"},{"v":"35.2","u":"far","n":"temperature"},{"v":"12","u":"per","n":"humidity"},{"v":"713","u":"per","n":"light"},{"v":"305.01","u":"per","n":"dust"},{"v":"20","u":"per","n":"airquality_raw"}],"bt":1422748800000})

smpp = SenMLParsePRED()
print(smpp.senmlparse(test_input1))
print(smpp.senmlparse(test_input2))
print(smpp.senmlparse(test_input3))
print(smpp.senmlparse(test_input4))
print(smpp.senmlparse(test_input5))
def TableRead(_input, filename = 'training_data_random.txt'):
    # _input as a timer
    msgId = _input[0]
    f = open(filename, 'r')
    line = f.readline()
    traindata_x = []
    traindata_y = []
    while line:
        line = line[:-1].split(',')
        training_x.append(line[:5])
        training_y.append(line[5:])
    	line = f.readline()
    for row in reader:
        traindata_x.append(row)
    traindata = (traindata_x, traindata_y)
    f.close()
    return (msgId, traindata)

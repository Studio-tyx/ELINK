def ModelWrite(_input):
    msgId = _input[0]
    model = _input[1]
    filename = _input[3]
    f = open(filename, 'wb')
    f.write(model)
    f.close()
    return (msgId, filename)
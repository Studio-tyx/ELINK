import matplotlib.pyplot as plt
import io
def plot(_map):
    fig = plt.figure(figsize = (8, 6))
    plt.title('title')
    plt.xlabel('X')
    plt.ylabel('Y')

    x_data = []
    y_data = []
    for key in _map.keys():
        obsType = key
        queue = _map[key]
        while not queue.empty():
            e = queue.get()
            x_data.append(e[1])
            y_data.append(e[0])
        plt.plot(x_data, y_data)

    plt.plot(x_data, y_data)
    canvas = fig.canvas
    buffer = io.BytesIO()
    canvas.print_figure(buffer)
    byte_img = buffer.getvalue()
    buffer.close()
    return byte_img

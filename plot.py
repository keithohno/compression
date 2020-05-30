import matplotlib.pyplot as plt
import numpy as np
import sys
import os

def main():
    plot_all(sys.argv[1])

def extract(line):
    split = line.split()
    return (int(split[0]), float(split[1]), int(split[2]))

def to_num(csize):
    return int(csize.replace('K','000'))

def plot_all(folder):
    csizes = os.listdir(folder)
    csizes.sort(key=to_num)
    fig, axs = plt.subplots(1, len(csizes))

    for i, csize in enumerate(csizes):
        f = open("{}/{}".format(folder, csize), 'r')
        data = list(map(extract, f.readlines()))

        origs = map(lambda x: x[2], data)
        med = np.median(list(origs))

        xs = []
        ys = []
        for d in data:
            if abs(d[2] - med) < 50000:
                xs.append(d[0])
                ys.append(d[1])

        xs = np.array(xs)
        ys = np.array(ys)
        m, b = np.polyfit(xs, ys, 1)


        axs[i].set_title(csize)
        axs[i].plot(xs, ys, '.')
        axs[i].plot(xs, b + m * xs)

    plt.show()

if __name__ == '__main__':
    main()

import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob


def main():
    plot_all()


def extract(line):
    split = line.split()
    return (int(split[0]), float(split[1]), int(split[2]), float(split[3]))


def csize_to_num(csize):
    return float(csize.replace('K', '000'))


def dir_to_num(dir_):
    return int(dir_.split('_cons')[1].split('-')[0])


def plot_all():
    dirs = glob.glob("test/test1/*")
    dirs.sort(key=dir_to_num)
    csizes = os.listdir("{}/lz4/".format(dirs[0]))
    csizes.sort(key=csize_to_num)
    fig, axs = plt.subplots(1, len(csizes), sharey=True)
    colors = ['#FF9999', '#DD7799', '#BB55BB', '#9933DD', '#7711FF']

    for i, csize in enumerate(csizes):
        xs = []
        ys = []
        for j, dir_ in enumerate(dirs):
            f = open("{}/lz4/{}".format(dir_, csize), 'r')
            data = list(map(extract, f.readlines()))
            for d in data:
                xs.append(d[3])
                ys.append(d[2])
        m, b = np.polyfit(xs, ys, 1)
        for j, dir_ in enumerate(dirs):
            f = open("{}/lz4/{}".format(dir_, csize), 'r')
            data = list(map(extract, f.readlines()))
            xs = []
            ys = []
            for d in data:
                xs.append(d[3])
                y_pred = m * d[3] + b
                ys.append(d[2] - y_pred)
                # ys.append(d[2])
            xs = np.array(xs)
            axs[i].plot(xs, ys, '.', color=colors[j], ms=8)
        axs[i].set_title("{}".format(csize))

    for i, dir_ in enumerate(dirs):
        print("{} - {}", colors[i], dir_)

    plt.show()


if __name__ == '__main__':
    main()

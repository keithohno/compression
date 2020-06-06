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


def dir_to_num(dir_, prefix):
    return "{}K".format(int(dir_.split(prefix)[1]) * 100)


def plot_all():
    dirs = glob.glob("test/test1/*")
    dirs = dirs[1:]
    csizes = os.listdir("{}/lz4/".format(dirs[0]))
    csizes.sort(key=csize_to_num)
    fig, axs = plt.subplots(len(csizes), len(dirs), sharex=True)

    for i, dir_ in enumerate(dirs):
        for j, csize in enumerate(csizes):
            f = open("{}/lz4/{}".format(dir_, csize), 'r')
            data = list(map(extract, f.readlines()))

            xs = []
            ys = []
            for d in data:
                xs.append(d[3])
                ys.append(d[1])

            axs[j].set_title("{} -- {}".format(dir_, csize))
            axs[j].plot(xs, ys, '.')

    plt.show()


if __name__ == '__main__':
    main()

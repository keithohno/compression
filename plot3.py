import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob


def main():
    plot_all('zstd', ['c', 'n', 'p', 'u'], resid=False)


def extract(line):
    return list(map(lambda x: int(x), line.split()[0:-1]))


def dir_filter(prefixes):
    return lambda dir_: dir_.split('/')[-1][0] in prefixes


def plot_all(ctype, prefixes, resid=True):
    dirs = glob.glob("test/test3/*")
    dirs = list(filter(dir_filter(prefixes), dirs))
    dirs.sort()
    fig, axs = plt.subplots(11, len(dirs), sharey=True)

    for i, dir_ in enumerate(dirs):
        f = open("{}/breakdown".format(dir_), 'r')
        data = list(map(extract, f.readlines()))
        for j, d in enumerate(data):
            axs[j][i].plot(d)
        axs[0][i].set_title(dir_.split('/')[-1])
    plt.show()


if __name__ == '__main__':
    main()

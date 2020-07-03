import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob


def extract(line):
    return list(map(lambda x: int(x), line.split()[0:-1]))


def plot():
    plot_size()


def plot_size():
    dirs = glob.glob("res/test6a/*")
    dirs = list(filter(lambda x: x.split('/')[-1] != 'status', dirs))
    dirs.sort(key=lambda x: int(x.split('fc')[-1]))
    fig, axs = plt.subplots(len(dirs), sharex=True)

    for i, dir_ in enumerate(dirs):
        f = open("{}/lz4".format(dir_), 'r')
        xs = []
        ys = []
        for j, l in enumerate(f.readlines()):
            xs.append(j)
            ys.append(int(l.split()[-1]))
            axs[i].plot(xs, ys, '-', markersize=1)
        axs[i].set_title("field count: " + dir_.split('fc')[-1])
    plt.show()


def plot_diff():
    dirs = glob.glob("res/test6a/*")
    dirs = list(filter(lambda x: x.split('/')[-1] != 'status', dirs))
    dirs.sort(key=lambda x: int(x.split('fc')[-1]))
    fig, axs = plt.subplots(len(dirs), sharex=True)

    for i, dir_ in enumerate(dirs):
        f = open("{}/lz4".format(dir_), 'r')
        xs = []
        ys = []
        lines = f.readlines()
        lines1 = lines[:-1]
        lines2 = lines[1:]
        for j in range(len(lines1)):
            xs.append(j)
            l1 = map(lambda x: int(x), lines1[j].split())
            l2 = map(lambda x: int(x), lines2[j].split())
            ys.append(diff(l1, l2))
            axs[i].plot(xs, ys, '-', markersize=1)
        axs[i].set_title("field count: " + dir_.split('fc')[-1])
    plt.show()


def plot_comp(ctype):
    dirs = glob.glob("res/test6a/*")
    dirs = list(filter(lambda x: x.split('/')[-1] != 'status', dirs))
    dirs.sort(key=lambda x: int(x.split('fc')[-1]))
    csizes = len(
        open("{}/{}".format(dirs[0], ctype), 'r').readlines()[0].split()) - 1
    fig, axs = plt.subplots(len(dirs), csizes, sharex=True)

    for i, dir_ in enumerate(dirs):
        for j in range(csizes):
            f = open("{}/{}".format(dir_, ctype), 'r')
            xs = []
            ys = []
            for k, l in enumerate(f.readlines()):
                xs.append(k)
                words = l.split()
                ys.append(int(words[-1]) / int(words[j]))
            axs[i][j].plot(xs, ys, '-', markersize=1)
            axs[i][j].set_title("field count: " + dir_.split('fc')[-1])
    plt.show()


def plot_lz4():
    plot_comp('lz4')


def plot_zstd():
    plot_comp('zstd')

import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob
from utils import misc

test = "ctest5"


def diff(l1, l2):
    res = []
    for i in range(len(l2)):
        res.append(l1[i] - l2[i])
    return res


def sqsum(l):
    tot = 0
    for i in l:
        tot += i*i
    return np.sqrt(tot)


def norm(l):
    sq = sqsum(l)
    res = []
    for i in l:
        res.append(i / sq)
    return res


def div(l, d):
    res = []
    for i in l:
        res.append(d / i)
    return res


def av(l):
    return sum(l) / len(l)


def diff_norm(l1, l2):
    return diff(norm(l1), norm(l2))


def plot_all():
    plot_dists("zt", 9, "Figure 1a: Distributions of Zeros By Sequence Length",
               file_name="res1a")
    plot_dists("zt", 40, "Figure 1b: Distributions of Zeros By Sequence Length",
               file_name="res1b")
    plot_dists(
        "nzt", 9, "Figure 2a: Distributions of Non-Zero Bytes By Sequence Length", file_name="res2a")
    plot_dists(
        "nzt", 9, "Figure 2b: Distributions of Non-Zero Bytes By Sequence Length", file_name="res2b")
    plot_one_dist(
        "nzt", "Figure 2c: Distribution of Non-Zero Bytes By Sequence Length", file_name="res2c")
    plot_one_dist(
        "nz", "Figure 2d: Distribution of Non-Zero Sequences By Sequence Length", file_name="res2d")
    plot_diff(
        'zt', "Figure 3a: Changes in Zero Sequence Distribution Over Time", file_name="res3a")
    plot_diff(
        'nzt', "Figure 3b: Changes in Non-Zero Sequence Distribution Over Time", file_name="res3b")
    plot_cdiff(
        'lz4', "Figure 4a: Changes in lz4 Compression Ratio Over Time", file_name="res4a")
    plot_cdiff(
        'zstd', "Figure 4b: Changes in zstd Compression Ratio Over Time", file_name="res4b")


def plot_one_dist(ztype, title, *, file_name=None):

    plt.figure(figsize=(10, 8))
    recct = 1500000
    data = misc.extract_file(
        "res/test5/rec{}/zeros/{}".format(recct, ztype))
    d = data[10]
    plt.bar(range(250)[1:][:110], d[:110])

    plt.xticks([i*10 for i in range(11)])

    plt.figtext(0.5, 0.04, "Sequence Length",
                ha="center", fontsize=12)
    plt.figtext(0.08, 0.5, "Frequency",
                va="center", fontsize=12, rotation="vertical")
    plt.figtext(0.5, 0.9, "{}".format(title),
                ha="center", va="bottom", fontsize=16)

    if file_name is None:
        plt.show()
    else:
        misc.savefig("res2/ctest5/" + file_name, plt)
    plt.clf()


def plot_dists(ztype, ysize, title, *, file_name=None):
    fig, axs = plt.subplots(ysize, 10, sharey=True,
                            figsize=(15, ysize * 3 / 4))

    for i in range(10):
        recct = (i+1) * 500000
        data = misc.extract_file(
            "res/test5/rec{}/zeros/{}".format(recct, ztype))
        for j in range(ysize):
            d = norm(data[j])
            axy = ysize - 1 - j
            axs[axy][i].plot(d)
            if axs[axy][i].is_first_col():
                axs[axy][i].set_ylabel(j + 1)
                axs[axy][i].set_yticklabels([])
            else:
                axs[axy][i].get_yaxis().set_visible(False)
            if axs[axy][i].is_last_row():
                axs[axy][i].set_xlabel(str(recct))
                axs[axy][i].set_xticklabels([])
            else:
                axs[axy][i].get_xaxis().set_visible(False)

    plt.figtext(0.5, -0.007 + 0.0024 * ysize, "# of Records",
                ha="center", fontsize=18)
    plt.figtext(0.08, 0.5, "Segment #",
                va="center", fontsize=18, rotation="vertical")
    plt.figtext(0.5, 0.9, "{}".format(title),
                ha="center", va="bottom", fontsize=24)

    if file_name is None:
        plt.show()
    else:
        misc.savefig("res2/ctest5/" + file_name, plt)
    plt.clf()


def plot_diff(ztype, title, *, file_name=None):
    colors = [[i / 10, 0, i / 20] for i in range(10)]
    plt.figure(figsize=(10, 8))
    for i in range(10):
        recct = (i+1) * 500000
        data = misc.extract_file(
            "res/test5/rec{}/zeros/{}".format(recct, ztype))
        d = []
        for j in range(len(data) - 1):
            d.append(sqsum(diff_norm(data[j+1], data[j])))
        plt.plot(range(len(d)), d, '-o', color=colors[i])

    plt.figtext(0.5, 0.01, "Segment #",
                ha="center", fontsize=12)
    plt.figtext(0.5, 0.9, "{}".format(title),
                ha="center", va="bottom", fontsize=16)

    if file_name is None:
        plt.show()
    else:
        misc.savefig("res2/ctest5/" + file_name, plt)
    plt.clf()


def plot_cdiff(ctype, title, *, file_name=None):
    colors = [[i / 10, 0, i / 20] for i in range(10)]

    for i in range(10):
        recct = (i+1) * 500000
        data = misc.extract_file("res/test5/rec{}/{}".format(recct, ctype))
        d = []
        for j in range(len(data) - 1):
            d.append(
                av(diff(div(data[j][:-1], data[j][-1]), div(data[-1][:-1], data[j][-1]))))
        plt.plot(range(len(d)), d, '-o', color=colors[i])

    plt.figtext(0.5, 0.03, "Segment #",
                ha="center", fontsize=12)
    plt.figtext(0.5, 0.9, "{}".format(title),
                ha="center", va="bottom", fontsize=16)

    if file_name is None:
        plt.show()
    else:
        misc.savefig("res2/ctest5/" + file_name, plt)
    plt.clf()

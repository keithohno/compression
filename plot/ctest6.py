import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob
from utils import misc


def normalize(list_):
    return np.true_divide(list_, np.linalg.norm(list_))


def plot_all():
    plot0()
    plot1()
    plot2()
    plot3()
    plot4()
    plot5()
    plot6()
    plot7()


def convolve(arr, kernel):
    res = np.convolve(arr, kernel, 'valid')
    lpad = int(len(kernel) / 2)
    rpad = len(kernel) - lpad
    res = np.insert(res, 0, [res[0]] * lpad)
    res = np.append(res, [res[-1]] * rpad)
    return res


def normalized_sum(lists):
    nlists = []
    for l in lists:
        nlists.append(normalize(l))
    return np.sum(nlists, axis=0)


def extract_size_diffs(prefix):
    diffs_list = []
    sizes = extract_sizes(prefix)
    for s in sizes:
        diffs_list.append(np.abs(convolve(s, [1, -1])))

    return diffs_list


def extract_size_diff_sum(prefix):
    diffs = extract_size_diffs(prefix)
    return normalized_sum(diffs)


def extract_sizes(prefix):
    dirs = glob.glob("res/test6/{}/*".format(prefix))
    dirs = sorted(list(filter(lambda x: x.split('/')[-1] != 'status', dirs)))

    sizes_list = []
    for i, dir_ in enumerate(dirs):
        data = misc.extract_file("{}/lz4".format(dir_))
        sizes = np.zeros(500)
        for j, d in enumerate(data):
            sizes[j] = d[-1]
        sizes_list.append(sizes)

    return sizes_list


def extract_comps(ctype):
    nsizes = []

    for prefix in ['a', 'b', 'c', 'd', 'e']:
        dirs = glob.glob("res/test6/{}/*".format(prefix))
        dirs = sorted(
            list(filter(lambda x: x.split('/')[-1] != 'status', dirs)))

        sizes_list = []
        for i, dir_ in enumerate(dirs):
            data = misc.extract_file("{}/lz4".format(dir_))
            sizes = np.zeros(500)
            for j, d in enumerate(data):
                sizes[j] = d[-1] / sum(d[:-1]) * 5
            sizes_list.append(sizes)

        for s in sizes_list:
            nsizes.append(s)

    return np.true_divide(np.sum(nsizes, axis=0), len(nsizes))


def plot0():
    fig, axs = plt.subplots(5, figsize=(4, 5.4))
    plt.subplots_adjust(top=0.87, bottom=0.05, left=0.1,
                        right=0.93, hspace=0.4)
    diffs = extract_sizes('a')
    for i, diff in enumerate(diffs):
        axs[i].plot(range(len(diff)), diff, '-')
        axs[i].set_title("Fields per Record: {}".format(i + 1))
        if i != 4:
            axs[i].set_xticklabels([])
    plt.suptitle("Figure 1a: Memory Consumption vs Field Size")
    misc.savefig("res2/ctest6/res0", plt)
    plt.close()


def plot1():
    fig, axs = plt.subplots(5, figsize=(4, 5.4))
    plt.subplots_adjust(top=0.87, bottom=0.05, left=0.1,
                        right=0.93, hspace=0.4)
    diffs = extract_size_diffs('a')
    for i, diff in enumerate(diffs):
        axs[i].plot(range(len(diff)), diff, '-')
        axs[i].set_title("Fields per Record: {}".format(i + 1))
        if i != 4:
            axs[i].set_xticklabels([])
    plt.suptitle("Figure 1b: Memory Consumption Differentials")
    misc.savefig("res2/ctest6/res1", plt)
    plt.close()


def plot2():
    fig, ax = plt.subplots(1, figsize=(4, 1.8))
    plt.subplots_adjust(top=0.8, bottom=0.12, left=0.1, right=0.93)
    diff = extract_size_diff_sum('a')
    ax.plot(range(len(diff)), diff, '-')
    plt.suptitle(
        "Figure 1c: Memory Differential Sum")
    misc.savefig("res2/ctest6/res2", plt)
    plt.close()


def plot3():
    fig, axs = plt.subplots(5, figsize=(4, 5.4))
    plt.subplots_adjust(top=0.87, bottom=0.05, left=0.1,
                        right=0.93, hspace=0.4)
    for i, prefix in enumerate(['a', 'b', 'c', 'd', 'e']):
        diff = extract_size_diff_sum(prefix)
        axs[i].plot(range(len(diff)), diff, '-')
        axs[i].set_title("Experiment {}".format(i))
        axs[i].set_yticklabels([])
        if i != 4:
            axs[i].set_xticklabels([])
    plt.suptitle(
        "Figure 2a: Memory Differential Sum\nAll Experiments")
    misc.savefig("res2/ctest6/res3", plt)
    plt.close()


def plot4():
    fig, ax = plt.subplots(1, figsize=(4, 1.8))
    plt.subplots_adjust(top=0.8, bottom=0.12, left=0.1, right=0.93)
    diffs = []
    for i, prefix in enumerate(['a', 'b', 'c', 'd', 'e']):
        diffs.append(extract_size_diff_sum(prefix))
    diff = normalized_sum(diffs)
    ax.plot(range(len(diff)), diff, '-')
    plt.suptitle(
        "Figure 2b: Memory Differential Sum\nAll Experiments Sum")
    misc.savefig("res2/ctest6/res4", plt)
    plt.close()
    for i, d in enumerate(diff):
        if d > .2:
            print(i)


def plot5():
    fig, ax = plt.subplots(1, figsize=(4, 1.8))
    plt.subplots_adjust(top=0.8, bottom=0.12, left=0.1, right=0.93)
    comp = extract_comps('lz4')
    ax.plot(range(len(comp)), comp, '-')
    ax.set_ylim(ymin=1)
    plt.suptitle(
        "Figure 3a: Aggregate lz4 Compression Ratio\nvs Field Size")
    misc.savefig("res2/ctest6/res5", plt)
    plt.close()


def plot6():
    fig, ax = plt.subplots(1, figsize=(4, 1.8))
    plt.subplots_adjust(top=0.8, bottom=0.12, left=0.1, right=0.93)
    comp = extract_comps('zstd')
    comp = np.abs(convolve(comp, [1, -1]))
    ax.plot(range(len(comp)), comp, '-')
    # ax.set_ylim(ymin=1)
    plt.suptitle(
        "Figure 3b: Aggregate zstd Compression Ratio\nvs Field Size")
    misc.savefig("res2/ctest6/res6", plt)
    plt.close()


def plot7():
    data = misc.extract_file("res/test6/a/fc5/zeros/zt")

    fig, axs = plt.subplots(50, 10, figsize=(15, 30))
    plt.subplots_adjust(top=0.99, bottom=0.01, left=0.01, right=0.99)

    for j in range(10):
        for i in range(50):
            d = data[j*50 + i][:-1]
            axs[i][j].plot(range(len(d)), d)
            axs[i][j].get_xaxis().set_visible(False)
            axs[i][j].get_yaxis().set_visible(False)
    misc.savefig("res2/ctest6/res7", plt)
    plt.close()

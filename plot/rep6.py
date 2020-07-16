import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import glob
import math
from utils import misc


def merge_bins(arr):
    nbins = int(math.ceil(len(arr) / 5))
    return [sum(arr[i*5:i*5+5]) for i in range(nbins)]


def sum_norm(arr):
    tot = 0
    for i in arr:
        tot += i
    return np.true_divide(arr, tot)


def extract_sizes(*, subtest=7):
    data = misc.extract_file("res/test{}/lz4".format(subtest))
    res = []
    for d in data:
        res.append(d[-1])
    return np.array(res)


def extract_ztotals(*, low=0, high=251, subtest=7):
    data = misc.extract_file("res/test{}/zeros/zt".format(subtest))
    res = []
    for d in data:
        res.append(sum(d[low:high]))
    return np.array(res)


def extract_zpercs(low=0, high=251, *, subtest=7):
    ztotals = extract_ztotals(low=low, high=high, subtest=subtest)
    sizes = extract_sizes(subtest=subtest)
    return np.true_divide(ztotals, sizes)


def plot_all():
    plot6a1()
    plot6a2()
    plot6a3()
    plot6a4()
    plot6b1()
    plot6b2()
    plot6b3()
    plot6b4()
    plot6b5()
    plot6b6()
    plot6b7()
    plot6c1()
    plot6c2()
    plot6c3()


def bar1234(data, binned_data):
    fig, ax = plt.subplots(1, figsize=(9, 4), sharey=True)
    plt.subplots_adjust(top=0.86, bottom=0.12, left=0.1, right=0.93)
    colors = misc.palette(5)

    ax.bar([x*5 for x in range(len(binned_data))], binned_data,
           color=colors[3], align='edge', width=5)
    ax.bar([x for x in range(len(data))],
           data, color=colors[1], align='edge')
    ax.set_ylabel("% of Zero Bytes")
    ax.set_xlabel("Zero Sequence Length")
    ax.set_ylim(top=.6)
    ax.xaxis.set_minor_locator(mpl.ticker.MultipleLocator(10))
    ax.xaxis.set_major_locator(mpl.ticker.MultipleLocator(50))
    return ax


def plot6a1():
    data = np.zeros(249, dtype=np.float64)
    for i in range(10):
        recct = (i+1) * 500000
        zdata = misc.extract_file("res/test5/rec{}/zeros/zt".format(recct))
        for z in zdata:
            data = np.add(data, z)
    data = np.true_divide(data, sum(data))
    bdata = merge_bins(data)

    ax = bar1234(data, bdata)
    ax.set_title("Figure 1: Zero Distribution (Report 4 Global Average)")
    misc.savefig("rep/rep6a/fig1", plt)
    plt.close()


def plot6a2():
    data = np.zeros(119, dtype=np.float64)
    c = 0
    for prefix in ['a', 'b', 'c', 'd', 'e']:
        for fc in range(1, 6):
            ef_data = misc.extract_file(
                'res/test6/{}/fc{}/zeros/zt'.format(prefix, fc))
            for efs_data in ef_data:
                data = np.add(data, efs_data)
                c += 1
    data = np.true_divide(data, c)
    data = np.true_divide(data, sum(data))
    data = data.tolist()
    data = data[:-1] + [0] * 130 + [data[-1]]
    bdata = merge_bins(data)

    ax = bar1234(data, bdata)
    ax.set_title("Figure 2: Zero Distribution (Report 5 Global Average)")

    misc.savefig("rep/rep6a/fig2", plt)
    plt.close()


def plot6a3():
    colors = misc.palette(7)
    fig, axs = plt.subplots(5, figsize=(9, 8), sharey=True, sharex=True)
    plt.subplots_adjust(top=0.9, bottom=0.08, left=0.1,
                        right=0.93, hspace=0.4)
    for j, prefix in enumerate(['a', 'b', 'c', 'd', 'e']):
        for i in range(5):
            ztots = misc.extract_file(
                "res/test6/{}/fc{}/zeros/zt".format(prefix, i+1))
            ztots = [sum(x) for x in ztots]
            sizes = misc.extract_file(
                "res/test6/{}/fc{}/lz4".format(prefix, i+1))
            sizes = [x[-1] for x in sizes]
            zpercs = np.true_divide(ztots, sizes)
            axs[j].plot(range(len(zpercs)), zpercs, '-', color=colors[i])
        axs[j].set_title("Part {}".format(j + 1))
    plt.suptitle(
        "Figure 3: Zero Percentage vs Field Size (Report 5)".format(prefix, j + 1))
    axs[2].set_ylabel("Zero Percentage")
    axs[-1].set_xlabel("Field Size")
    leg = axs[0].legend(["fields per rec: {}".format(x + 1)
                         for x in range(5)], loc='upper right')
    for line in leg.get_lines():
        line.set_linewidth(3.0)
    misc.savefig("rep/rep6a/fig3", plt)
    plt.close()


def plot6a4():
    colors = misc.palette(4)
    fig, ax = plt.subplots(1, figsize=(9, 4), sharey=True)
    plt.subplots_adjust(top=0.9, bottom=0.18, left=0.1, right=0.93)
    allpercs = []
    for j, prefix in enumerate(['a', 'b', 'c', 'd', 'e']):
        for i in range(5):
            ztots = misc.extract_file(
                "res/test6/{}/fc{}/zeros/zt".format(prefix, i+1))
            ztots = [sum(x) for x in ztots]
            sizes = misc.extract_file(
                "res/test6/{}/fc{}/lz4".format(prefix, i+1))
            sizes = [x[-1] for x in sizes]
            zpercs = np.true_divide(ztots, sizes)
            for z in zpercs:
                allpercs.append(z)
    ax.hist(allpercs, weights=np.zeros_like(allpercs) +
            1. / len(allpercs), color=colors[1], bins=[0.05*x for x in range(3, 16)])
    print("6a - 4")
    print("std: {}".format(round(np.std(allpercs), 3)))
    print("av: {}".format(round(np.mean(allpercs), 3)))
    ax.set_ylabel("Relative Frequency")
    ax.set_xlabel("Zero Percentage")
    plt.suptitle(
        "Figure 4: Zero Percentage Frequency Distribution (Report 5)".format(prefix, j + 1))
    misc.savefig("rep/rep6a/fig4", plt)


def plot6b1():
    fig, ax = plt.subplots(1, figsize=(9, 2.8), sharey=True)
    plt.subplots_adjust(top=0.87, bottom=0.16, left=0.1, right=0.93)
    sizes = extract_sizes()
    ax.plot(range(len(sizes)), sizes, '-', color=misc.palette(3)[0])
    ax.set_xlabel("Record Count (x1000)")
    ax.set_ylabel("Total Memory Consumption (Bytes)")
    ax.set_title("Figure 1: Memory Consumption vs Record Count")
    misc.savefig("rep/rep6b/fig1", plt)
    plt.close()


def plot6b2():
    fig, ax = plt.subplots(1, figsize=(9, 4))
    plt.subplots_adjust(top=0.87, bottom=0.12, left=0.1, right=0.93)
    colors = misc.palette(4)
    ztots0 = extract_ztotals(low=10)
    ztots1 = extract_ztotals(low=10, high=-1)
    ztots2 = extract_ztotals(low=-1)
    ax.plot(range(len(ztots0)), ztots0, '-', color=colors[0])
    ax.plot(range(len(ztots1)), ztots1, '-', color=colors[1])
    ax.plot(range(len(ztots2)), ztots2, '-', color=colors[2])
    ax.set_xlabel("Record Count (x1000)")
    ax.set_ylabel("# of Zero Bytes")
    ax.set_title("Figure 2: Zero Bytes vs Record Count")
    leg = ax.legend(["sequence length: 10+",
                     "sequence length: 10 - 249",
                     "sequence length: 250+"])
    for line in leg.get_lines():
        line.set_linewidth(3.0)
    misc.savefig("rep/rep6b/fig2", plt)
    plt.close()


def plot6b3():
    fig, ax = plt.subplots(1, figsize=(9, 4))
    plt.subplots_adjust(top=0.87, bottom=0.12, left=0.1, right=0.93)
    colors = misc.palette(4)
    zpercs0 = extract_zpercs(low=10)
    zpercs1 = extract_zpercs(low=10, high=-1)
    zpercs2 = extract_zpercs(low=-1)
    ax.plot(range(len(zpercs0)), zpercs0, '-', color=colors[0])
    ax.plot(range(len(zpercs1)), zpercs1, '-', color=colors[1])
    ax.plot(range(len(zpercs2)), zpercs2, '-', color=colors[2])
    leg = ax.legend(["sequence length: 10+",
                     "sequence length: 10 - 249",
                     "sequence length: 250+"])
    ax.set_xlabel("Record Count (x1000)")
    ax.set_ylabel("Fraction of Total Bytes")
    ax.set_title("Figure 3: Zero Percentage vs Record Count")
    misc.savefig("rep/rep6b/fig3", plt)
    plt.close()


def plot6b4():
    data = misc.extract_file("res/test7/zeros/zt")
    data_sum = np.zeros(len(data[0]))
    for i in range(500):
        data_sum = np.add(data_sum, data[i])
    data_av = sum_norm(data_sum)
    bdata_av = merge_bins(data_av)

    ax = bar1234(data_av, bdata_av)
    ax.set_title("Figure 4: Relative Frequency of Zero Bytes (Low Memory)")

    misc.savefig("rep/rep6b/fig4", plt)
    plt.close()


def plot6b5():
    data = misc.extract_file("res/test7/zeros/zt")
    data_sum = np.zeros(len(data[0]))
    for i in range(500):
        data_sum = np.add(data_sum, data[-i])
    data_av = sum_norm(data_sum)
    bdata_av = merge_bins(data_av)

    ax = bar1234(data_av, bdata_av)
    ax.set_title("Figure 5: Relative Frequency of Zero Bytes (High Memory)")

    misc.savefig("rep/rep6b/fig5", plt)
    plt.close()


def hist6b67(zpercs):
    fig, ax = plt.subplots(1, figsize=(9, 4))
    plt.subplots_adjust(top=0.87, bottom=0.12, left=0.1, right=0.93)
    colors = misc.palette(4)
    ax.hist(zpercs, weights=np.zeros_like(zpercs) +
            1. / len(zpercs), color=colors[1], bins=[0.025*x for x in range(11, 41)])
    print("std: {}".format(round(np.std(zpercs), 3)))
    print("av: {}".format(round(np.mean(zpercs), 3)))
    ax.set_ylabel("Relative Frequency")
    ax.set_xlabel("Zero Percentage")


def plot6b6():
    print("6b - 6")
    zpercs = extract_zpercs(low=10)[:500]
    hist6b67(zpercs)
    plt.suptitle(
        "Figure 6: Zero Percentage Frequency Distribution (Low Memory)")
    misc.savefig("rep/rep6b/fig6", plt)
    plt.close()


def plot6b7():
    print("6b - 7")
    zpercs = extract_zpercs(low=10)[-500:]
    hist6b67(zpercs)
    plt.suptitle(
        "Figure 7: Zero Percentage Frequency Distribution (Low Memory)")
    misc.savefig("rep/rep6b/fig7", plt)
    plt.close()


def plot6c1():
    print("6c - 1")
    zpercs = extract_zpercs(low=10, subtest=8)[:500]
    fig, ax = plt.subplots(1, figsize=(9, 4))
    plt.subplots_adjust(top=0.87, bottom=0.12, left=0.1, right=0.93)
    colors = misc.palette(4)
    ax.hist(zpercs, weights=np.zeros_like(zpercs) +
            1. / len(zpercs), color=colors[1], bins=[0.01*x for x in range(12, 31)])
    print("std: {}".format(round(np.std(zpercs), 3)))
    print("av: {}".format(round(np.mean(zpercs), 3)))
    ax.set_ylabel("Relative Frequency")
    ax.set_xlabel("Zero Percentage")
    plt.suptitle(
        "Figure 1: Zero Percentage Frequency Distribution")
    misc.savefig("rep/rep6c/fig1", plt)
    plt.close()


def plot6c2():
    data = misc.extract_file("res/test8/zeros/zt")
    lz4_data = misc.extract_file("res/test8/lz4")
    size_data = [x[-1] for x in lz4_data]

    data_sum = np.zeros(len(data[0]))
    for d, s in zip(data, size_data):
        if s < 13000000000:
            data_sum = np.add(data_sum, d)
    data_av = sum_norm(data_sum)
    bdata_av = merge_bins(data_av)

    ax = bar1234(data_av, bdata_av)
    ax.set_title(
        "Figure 2: Relative Frequency of Zero Bytes (12.7GB Workloads)")

    misc.savefig("rep/rep6c/fig2", plt)
    plt.close()


def plot6c3():
    data = misc.extract_file("res/test8/zeros/zt")
    lz4_data = misc.extract_file("res/test8/lz4")
    size_data = [x[-1] for x in lz4_data]

    data_sum = np.zeros(len(data[0]))
    for d, s in zip(data, size_data):
        if s > 13000000000:
            data_sum = np.add(data_sum, d)
    data_av = sum_norm(data_sum)
    bdata_av = merge_bins(data_av)

    ax = bar1234(data_av, bdata_av)
    ax.set_title(
        "Figure 3: Relative Frequency of Zero Bytes (14.9GB Workloads)")

    misc.savefig("rep/rep6c/fig3", plt)
    plt.close()

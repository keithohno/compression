import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob


def extract(line):
    return list(map(lambda x: int(x), line.split()[0:-1]))


def plot_all():
    dirs = glob.glob("res/test4/*")
    dirs = list(filter(lambda x: x.split('/')[-1] != 'status', dirs))
    dirs.sort(key=lambda x: int(x.split('rec')[-1]))
    fig, axs = plt.subplots(15, 10, sharey=True)

    for i, dir_ in enumerate(dirs):
        f = open("{}/zt".format(dir_), 'r')
        recct = int(dir_.split('rec')[-1])
        for j, l in enumerate(f.readlines()):
            data = list(map(lambda x: int(x) * 500000 / recct, l.split()))
            axs[j][i].plot(data)
        axs[0][i].set_title(dir_.split('rec')[-1])
    plt.show()

import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob


def main():
    plot_all('zstd', ['c', 'n', 'p', 'u'], resid=False)


def extract(line):
    split = line.split()
    return (int(split[0]), float(split[1]), int(split[2]), int(split[3]), float(split[4]))


def csize_to_num(csize):
    return float(csize.replace('K', '000'))


def dir_filter(prefixes):
    return lambda dir_: dir_.split('/')[-1][0] in prefixes


def plot_all(ctype, prefixes, resid=True):
    dirs = glob.glob("test/test2/*")
    dirs = list(filter(dir_filter(prefixes), dirs))
    dirs.sort()
    csizes = os.listdir("{}/lz4/".format(dirs[0]))
    csizes.sort(key=csize_to_num)
    fig, axs = plt.subplots(1, len(csizes), sharey=True)
    colors = [(.5 * i / len(dirs), i / len(dirs), i / len(dirs))
              for i in range(len(dirs))]
    for i, csize in enumerate(csizes):
        xs = []
        ys = []
        for k, dir_ in enumerate(dirs):
            f = open("{}/{}/{}".format(dir_, ctype, csize), 'r')
            data = list(map(extract, f.readlines()))
            for d in data:
                xs.append(d[4])
                ys.append(d[2])
        m, b = np.polyfit(xs, ys, 1)
        for j, dir_ in enumerate(dirs):
            f = open("{}/{}/{}".format(dir_, ctype, csize), 'r')
            data = list(map(extract, f.readlines()))
            xs = []
            ys = []
            for d in data:
                xs.append(d[4])
                y_pred = m * d[4] + b
                if resid:
                    ys.append(d[2] - y_pred)
                else:
                    ys.append(d[2])
            xs = np.array(xs)
            axs[i].plot(xs, ys, '.', color=colors[j], ms=2)
        # if not resid:
        #     axs[i].plot(xs, m*xs + b)
        axs[i].set_title("{}".format(csize))

    for i, dir_ in enumerate(dirs):
        print("{} - {}", colors[i], dir_)

    plt.show()


if __name__ == '__main__':
    main()

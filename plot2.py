import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob


def main():
    plot_all()


def extract(line):
    split = line.split()
    return (int(split[0]), float(split[1]), int(split[2]), int(split[3]), float(split[4]))


def csize_to_num(csize):
    return float(csize.replace('K', '000'))


def dir_to_num(dir_):
    return int(dir_.split('_cons')[1].split('-')[0])


def plot_all():
    dirs = glob.glob("test/test2/*")
    # dirs.sort(key=dir_to_num)
    csizes = os.listdir("{}/lz4/".format(dirs[0]))
    csizes.sort(key=csize_to_num)
    fig, axs = plt.subplots(2, len(csizes), sharey=True)
    colors = ['#FF5533', '#DD5555', '#BB5577',
              '#995599', '#7755BB', '#5555DD', '#3355FF']

    for i, csize in enumerate(csizes):
        for j, ctype in enumerate(['lz4', 'zstd']):
            xs = []
            ys = []
            for k, dir_ in enumerate(dirs):
                f = open("{}/{}/{}".format(dir_, ctype, csize), 'r')
                data = list(map(extract, f.readlines()))
                for d in data:
                    xs.append(d[4])
                    ys.append(d[1])
            m, b = np.polyfit(xs, ys, 1)
            for k, dir_ in enumerate(dirs):
                f = open("{}/{}/{}".format(dir_, ctype, csize), 'r')
                data = list(map(extract, f.readlines()))
                xs = []
                ys = []
                for d in data:
                    xs.append(d[4])
                    y_pred = m * d[3] + b
                    # ys.append(d[1] - y_pred)
                    ys.append(d[2])
                xs = np.array(xs)
                axs[j][i].plot(xs, ys, '-', color=colors[k], ms=1)
            axs[j][i].set_title("{}".format(csize))

    for i, dir_ in enumerate(dirs):
        print("{} - {}", colors[i], dir_)

    plt.show()


if __name__ == '__main__':
    main()

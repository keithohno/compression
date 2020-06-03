import matplotlib.pyplot as plt
import numpy as np
import sys
import os
import glob

def main():
    plot_all(sys.argv[1])

def extract(line):
    split = line.split()
    return (int(split[0]), float(split[1]), int(split[2]))

def csize_to_num(csize):
    return int(csize.replace('K','000'))

def dir_to_num(dir_, prefix):
    return int(dir_.split(prefix)[1])

def plot_all(prefix):
    dirs = glob.glob("{}*".format(prefix))
    dirs.sort()
    csizes = os.listdir(dirs[0])
    csizes.sort(key=csize_to_num)
    fig, axs = plt.subplots(len(dirs), len(csizes), sharex=True)

    for i, dir_ in enumerate(dirs):
        for j, csize in enumerate(csizes):
            f = open("{}/{}".format(dir_, csize), 'r')
            data = list(map(extract, f.readlines()))

            origs = map(lambda x: x[2], data)
            med = np.median(list(origs))

            xs = []
            ys = []
            for d in data:
                if abs(d[2] - med) < 100000000:
                    xs.append(d[0])
                    ys.append(d[1])

            xs = np.array(xs)/max(xs)
            ys = np.array(ys)
            m, b = np.polyfit(xs, ys, 1)


            axs[i][j].set_title("{}-{}".format(dir_to_num(dir_, prefix), csize))
            axs[i][j].get_xaxis().set_visible(False)
            axs[i][j].figure.set_size_inches(2, 2)
            axs[i][j].set_aspect(40, adjustable='datalim')
            axs[i][j].plot(xs, ys, '.')
            axs[i][j].plot(xs, b + m * xs)

    plt.show()

if __name__ == '__main__':
    main()

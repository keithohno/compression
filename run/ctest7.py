
import os
from functools import reduce
from utils import scripts, config, misc

scripts.build()


def run():
    configs = []
    for recct in [1000 * (i+1) for i in range(3000)]:
        configs.append(config.Config(recct=recct, opct=0, fav=107))

    folder = 'test7'
    status = misc.Status(folder, len(configs))

    while not status.finished:
        conf = configs[status.current]
        conf.load()
        # init
        print("\n{}:  recct={}".format(
            status.current, conf.recct))
        stopwatch = misc.Stopwatch()
        # flush, run, dump
        scripts.flush()
        stopwatch.lap(' - - flush: ')
        scripts.redis_all()
        stopwatch.lap(' - - workload: ')
        scripts.dump()
        stopwatch.lap(' - - dump: ')
        # zstd, lz4, zeros
        scripts.compress_zstd()
        stopwatch.lap(' - - zstd: ')
        scripts.compress_lz4()
        stopwatch.lap(' - - lz4: ')
        scripts.zero_count()
        stopwatch.lap(' - - zeros: ')
        # write into final res folder
        res_zstd_out(folder)
        res_lz4_out(folder)
        res_zeros_out(folder)
        status.inc()
        stopwatch.total(' - total: ')


def res_zeros_out(folder):
    # get output lines
    output = misc.read_file("out/zeros").splitlines()
    # add third word of each line to its corresponding output file
    for l in output:
        misc.append_file(
            "res/{}/zeros/{}".format(folder, l.split()[0]), l.split()[2] + ' ')
    # enter a newline in each file
    for suff in ['z', 'nz', 'zt', 'nzt']:
        misc.append_file(
            "res/{}/zeros/{}".format(folder, suff), '\n')


def res_zstd_out(folder):
    # get output lines
    output = misc.read_file("out/zstd").splitlines()
    # get sorted list of second words of each line
    output = sorted([int(x.split()[1]) for x in output])
    # reduce lists of nums to space separated string
    output = reduce(lambda t, s: str(t) + ' ' + str(s), output)
    # write to res file
    misc.append_file(
        "res/{}/zstd".format(folder), output + '\n')


def res_lz4_out(folder):
    # get output lines
    output = misc.read_file("out/lz4").splitlines()
    # get sorted list of second words of each line
    output = sorted([int(x.split()[1]) for x in output])
    # reduce lists of nums to space separated string
    output = reduce(lambda t, s: str(t) + ' ' + str(s), output)
    # write to res file
    misc.append_file(
        "res/{}/lz4".format(folder), output + '\n')

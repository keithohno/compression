
import os
from functools import reduce
from utils import scripts, config, misc

scripts.build()


def run():
    configs = []
    for fcount in [i + 1 for i in range(5)]:
        for fav in [(i + 100) for i in range(400)]:
            configs.append(config.Config(
                recct=1000000, opct=0, fcount=fcount, fav=fav, fdist='c'))

    status = misc.Status('test6', len(configs))

    while not status.finished:
        conf = configs[status.current]
        conf.load()
        print(status.current)
        # init
        print("\n{}:  fc={}  fa={}".format(
            status.current, conf.fcount, conf.fav))
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
        res_zstd_out(conf.recct)
        res_lz4_out(conf.recct)
        res_zeros_out(conf.recct)
        status.inc()
        stopwatch.total(' - total: ')


def res_zeros_out(fcount):
    # get output lines
    output = misc.read_file("out/zeros").splitlines()
    # add third word of each line to its corresponding output file
    for l in output:
        misc.append_file(
            "res/test6/fc{}/zeros/{}".format(fcount, l.split()[0]), l.split()[2] + ' ')
    # enter a newline in each file
    for suff in ['z', 'nz', 'zt', 'nzt']:
        misc.append_file(
            "res/test6/fc{}/zeros/{}".format(fcount, suff), '\n')


def res_zstd_out(fcount):
    # get output lines
    output = misc.read_file("out/zstd").splitlines()
    # get sorted list of second words of each line
    output = sorted([int(x.split()[1]) for x in output])
    # reduce lists of nums to space separated string
    output = reduce(lambda t, s: str(t) + ' ' + str(s), output)
    # write to res file
    misc.write_file(
        "res/test6/fc{}/zstd".format(fcount), output)


def res_lz4_out(fcount):
    # get output lines
    output = misc.read_file("out/lz4").splitlines()
    # get sorted list of second words of each line
    output = sorted([int(x.split()[1]) for x in output])
    # reduce lists of nums to space separated string
    output = reduce(lambda t, s: str(t) + ' ' + str(s), output)
    # write to res file
    misc.write_file(
        "res/test6/fc{}/lz4".format(fcount), output)

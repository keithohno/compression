
import os
from functools import reduce
from utils import scripts, config, misc

scripts.build()


def run():
    configs = []
    for recct in [(i + 1) * 500000 for i in range(10)]:
        configs.append(config.Config(
            recct=recct, opct=int(recct / 4), fcount=5, fav=100, fdist='c'))

    status = misc.Status('test5', len(configs))

    while not status.finished:
        conf = configs[status.current]
        conf.load()

        # consecutive workloads (without flush)
        for i in range(40):
            # init
            print("\n{}.{}:  R={}  O={}".format(
                status.current, i, conf.recct, recct*i))
            stopwatch = misc.Stopwatch()
            # flush and load/run
            if i == 0:
                scripts.flush()
                stopwatch.lap(' - - flush: ')
                scripts.redis_load()
            else:
                scripts.redis_run()
            stopwatch.lap(' - - workload: ')
            # dump
            scripts.dump()
            stopwatch.lap(' - - dump: ')
            # zstd
            scripts.compress_zstd()
            stopwatch.lap(' - - zstd: ')
            zstd_out = misc.read_file("out/zstd")
            # lz4
            scripts.compress_lz4()
            stopwatch.lap(' - - lz4: ')
            lz4_out = misc.read_file("out/lz4")
            # zeros
            scripts.zero_count()
            stopwatch.lap(' - - zeros: ')
            zeros_out = misc.read_file("out/zeros")
            # if first iter: reset, else: add SPLIT marker
            for suff in ['z', 'nz', 'zt', 'nzt', 'zstd', 'lz4']:
                if i == 0:
                    misc.write_file("out/test5_" + suff, "")
                else:
                    misc.append_file("out/test5_" + suff, "SPLIT")
            # save intermediate file outputs
            save_zeros_out(zeros_out)
            save_zstd_out(zstd_out)
            save_lz4_out(lz4_out)
            stopwatch.total(' - total: ')
        # write into final res folder
        res_zstd_out(conf.recct)
        res_lz4_out(conf.recct)
        res_zeros_out(conf.recct)
        status.inc()


def save_zeros_out(zeros_out):
    for l in zeros_out.splitlines():
        if len(l) > 0:
            misc.append_file("out/test5_" + l.split()[0], l + '\n')


def save_zstd_out(zstd_out):
    for l in zstd_out.splitlines():
        if len(l) > 0:
            misc.append_file("out/test5_zstd", l + '\n')


def save_lz4_out(lz4_out):
    for l in lz4_out.splitlines():
        if len(l) > 0:
            misc.append_file("out/test5_lz4", l + '\n')


def res_zeros_out(recct):
    # processing output
    for suff in ['z', 'nz', 'zt', 'nzt']:
        # split by SPLIT marker
        output = misc.read_file("out/test5_" + suff).split('SPLIT')
        # split by line
        output = [x.strip().splitlines() for x in output]
        # take third word of each line
        output = [[y.split()[2] for y in x] for x in output]
        # reduce lists of nums to space separated string
        output = [reduce(lambda t, s: t + ' ' + s, x) for x in output]
        # reduce list of strings to \n separated string
        output = reduce(lambda t, s: t + '\n' + s, output)
        misc.write_file(
            "res/test5/rec{}/zeros/{}".format(recct, suff), output)


def res_zstd_out(recct):
    # split by SPLIT marker
    output = misc.read_file("out/test5_zstd").split('SPLIT')
    # split by line
    output = [x.strip().splitlines() for x in output]
    # take second word of each line and sort
    output = [sorted([int(y.split()[1]) for y in x]) for x in output]
    # reduce lists of nums to space separated string
    output = [reduce(lambda t, s: str(t) + ' ' + str(s), x) for x in output]
    # reduce list of strings to \n separated string
    output = reduce(lambda t, s: t + '\n' + s, output)
    misc.write_file(
        "res/test5/rec{}/zstd".format(recct), output)


def res_lz4_out(recct):
    # split by SPLIT marker
    output = misc.read_file("out/test5_lz4").split('SPLIT')
    # split by line
    output = [x.strip().splitlines() for x in output]
    # take second word of each line and sort
    output = [sorted([int(y.split()[1]) for y in x]) for x in output]
    # reduce lists of nums to space separated string
    output = [reduce(lambda t, s: str(t) + ' ' + str(s), x) for x in output]
    # reduce list of strings to \n separated string
    output = reduce(lambda t, s: t + '\n' + s, output)
    misc.write_file(
        "res/test5/rec{}/lz4".format(recct), output)

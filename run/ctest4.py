import random
import os
from functools import reduce
from utils import scripts, config, misc

scripts.build()


def run():
    configs = []
    for recct in [(i + 1) * 500000 for i in range(10)]:
        configs.append(config.Config(
            recct=recct, opct=recct, fcount=5, fav=100, fdist='c'))

    status = misc.Status('test4', len(configs))

    while not status.finished:
        conf = configs[status.current]
        conf.load()
        print(status.current)

        # consecutive workloads (without flush)
        for i in range(15):
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
            # count zeros
            scripts.zero_count()
            out = misc.read_file("out/zeros")
            # if first iter: reset, else: add SPLIT marker
            for suff in ['z', 'nz', 'zt', 'nzt']:
                if i == 0:
                    misc.write_file("out/test4_" + suff, "")
                else:
                    misc.append_file("out/test4_" + suff, "SPLIT")
            # pass output to tmp files
            for l in out.splitlines():
                if len(l) > 0:
                    misc.append_file("out/test4_" + l.split()[0], l + '\n')
            stopwatch.lap(' - - zeros: ')
            stopwatch.total(' - total: ')

        # processing output
        for suff in ['z', 'nz', 'zt', 'nzt']:
            # split by SPLIT marker
            output = misc.read_file("out/test4_" + suff).split('SPLIT')
            # split by line
            output = [x.strip().splitlines() for x in output]
            # take third word of each line
            output = [[y.split()[2] for y in x] for x in output]
            # reduce lists of nums to space separated string
            output = [reduce(lambda t, s: t + ' ' + s, x) for x in output]
            # reduce list of strings to \n separated string
            output = reduce(lambda t, s: t + '\n' + s, output)
            misc.write_file(
                "res/test4/rec{}/{}".format(conf.recct, suff), output)
        status.inc()

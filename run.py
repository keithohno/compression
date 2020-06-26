import random
import os
from utils import scripts, coordinator as coord, misc

scripts.build()


class CTest4:

    @staticmethod
    def dir_name_fn(config):
        return "rct{}".format(config.recct)

    @staticmethod
    def zero_log(data, **kwargs):
        lines = data.strip().splitlines()
        output = {}
        for l in lines:
            words = l.strip().split()
            if not words[0] in output:
                output[words[0]] = ""
            output[words[0]] += "{} ".format(words[2])
        for k in output.keys():
            output[k] += '\n'
        return output


def ctest4():
    reccts = [(i + 1) * 500000 for i in range(10)]
    opct = 500000
    coordinator = coord.Coordinator("test4",
                                    recct=reccts, opct=opct, fcount=5, fdist='c', fav=100)
    coordinator.zero_log = CTest4.zero_log
    coordinator.dir_name_fn = CTest4.dir_name_fn
    while not coordinator.finished:
        c = coordinator.next()
        c.load()
        for i in range(10):
            print("\n{}.{}:  R={}  O={}".format(
                coordinator.status, i, c.recct, opct*i))
            stopwatch = misc.Stopwatch()
            if i == 0:
                scripts.flush()
                stopwatch.lap(' - - flush: ')
                scripts.redis_load()
            else:
                scripts.redis_run()
            stopwatch.lap(' - - workload: ')
            scripts.dump()
            stopwatch.lap(' - - dump: ')
            scripts.zero_count()
            coordinator.log()
            stopwatch.lap(' - - zeros: ')
            stopwatch.total(' - total: ')
        coordinator.inc()


ctest4()

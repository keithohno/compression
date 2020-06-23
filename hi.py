import os
from utils import scripts, config, misc


def init():
    os.environ["COMPRESSION_HOME"] = str(os.getcwd())
    scripts.build()


init()


def test():
    params_list = config.WLParams(
        recct=4000000, opct=12000000, fcount=5, fdist='c', fav=100).to_iter()
    for p in params_list:
        stopwatch = misc.Stopwatch()
        config.load_config(p)
        stopwatch.lap('config: ')
        scripts.flush()
        stopwatch.lap('flush: ')
        scripts.redis_run(False)
        stopwatch.lap('workload: ')
        scripts.dump()
        stopwatch.lap('dump: ')


test()

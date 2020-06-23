
import json
import itertools
import os


def listify(item):
    if isinstance(item, list):
        return item
    else:
        return [item]


class WLParams:
    def __init__(self, recct=1, opct=1, fcount=1, fdist='c', frange=1,
                 fav=1, fdens=1.0, fstd=1, param_list=None):
        if param_list is None:
            self.recct = recct
            self.opct = opct
            self.fcount = fcount
            self.fdist = fdist
            self.frange = frange
            self.fav = fav
            self.fdens = fdens
            self.fstd = fstd
        else:
            self.recct = param_list[0]
            self.opct = param_list[1]
            self.fcount = param_list[2]
            self.fdist = param_list[3]
            self.frange = param_list[4]
            self.fav = param_list[5]
            self.fdens = param_list[6]
            self.fstd = param_list[7]

    def to_list(self):
        return list(map(lambda x: listify(x), [self.recct, self.opct, self.fcount,
                                               self.fdist, self.frange, self.fav,
                                               self.fdens, self.fstd]))

    def to_iter(self):
        return itertools.product(*self.to_list())


def load_config(params):
    try:
        os.environ['COMPRESSION_HOME']
    except:
        print("ERR: need to set COMPRESSION_HOME")
        exit(1)
    if isinstance(params, tuple):
        params = WLParams(param_list=params)
    config = json.dumps({
        "record_count": params.recct,
        "operation_count": params.opct,
        "field_count": params.fcount,
        "field_dist": params.fdist,
        "field_range": params.frange,
        "field_av": params.fav,
        "field_density": params.fdens,
        "field_std": params.fstd
    })
    f = open(
        "{}/config/redis-loader.json".format(os.environ['COMPRESSION_HOME']), 'w+')
    f.write(config)
    f.close()

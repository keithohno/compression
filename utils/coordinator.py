
import json
import itertools
import os


sdir = os.path.dirname(os.path.realpath(__file__))


class WorkloadConfig:
    def __init__(self, recct=1, opct=1, fcount=1, fav=1, fdens=1.0, fdist='c', frange=1, fstd=1):
        self.recct = recct
        self.opct = opct
        self.fcount = fcount
        self.fdist = fdist
        self.frange = frange
        self.fav = fav
        self.fdens = fdens
        self.fstd = fstd

    def load(self):
        config_str = json.dumps({
            "record_count": self.recct,
            "operation_count": self.opct,
            "field_count": self.fcount,
            "field_dist": self.fdist,
            "field_range": self.frange,
            "field_av": self.fav,
            "field_density": self.fdens,
            "field_std": self.fstd
        })
        f = open("{}/../config/redis-loader.json".format(sdir), 'w+')
        f.write(config_str)
        f.close()


def listify(x):
    return x if isinstance(x, list) else [x]


def write_file(path, data):
    init_file(path)
    try:
        f = open((path), 'w')
        f.write(data)
    except Exception as e:
        print("ERR: file write error")
        print(str(e))
        exit(1)
    finally:
        f.close()


def append_file(path, data):
    init_file(path)
    try:
        f = open((path), 'a')
        f.write(data)
    except Exception as e:
        print("ERR: file append error")
        print(str(e))
        exit(1)
    finally:
        f.close()


def read_file(path):
    try:
        f = open((path), 'r')
        ret = f.read()
    except Exception as e:
        print("ERR: file read error")
        print(str(e))
        exit(1)
    finally:
        f.close()
    return ret


def init_file(path, *,  default=''):
    try:
        f = open((path), 'r')
    except Exception:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        f = open(path, 'w+')
        f.write(default)
    finally:
        f.close()


def init_status(path):
    init_file(path, default='0')
    return int(read_file(path))


class Coordinator:
    def __init__(self, name, *,
                 recct=1, opct=1, fcount=1, fav=1, fdens=1.0, fdist='c', frange=1, fstd=1,
                 lz4_log=None, zstd_log=None, zero_log=None, dir_name_fn=lambda x: ''):
        # generating list of all configurations
        wk_params = list(map(listify, [recct, opct, fcount, fav, fdens]))
        wk_params.append([])
        for dist in listify(fdist):
            if dist == 'c' or dist == 'p':
                wk_params[-1].append((dist, 1, 1))
            if dist == 'u':
                for r in listify(frange):
                    wk_params[-1].append(('u', r, 1))
            if dist == 'n':
                for s in listify(fstd):
                    wk_params[-1].append(('n', 1, s))
        wk_param_product = itertools.product(*wk_params)
        self.configs = list(map(lambda x: WorkloadConfig(
            x[0], x[1], x[2], x[3], x[4], x[5][0], x[5][1], x[5][2]), wk_param_product))
        # status
        self.name = name
        self.status = init_status(
            "{}/../test/{}/status".format(sdir, self.name))
        self.finished = False
        # variables used for logging
        self.lz4_log = lz4_log
        self.zstd_log = zstd_log
        self.zero_log = zero_log
        self.dir_name_fn = dir_name_fn

    def next(self):
        return self.configs[self.status]

    def log(self):
        config = self.configs[self.status]
        subdir = self.dir_name_fn(config)
        if callable(self.lz4_log):
            data = read_file("{}/../out/lz4".format(sdir))
            out = self.lz4_log(data, config=config, status=self.status)
            for k, v in out.items():
                append_file("{}/../test/{}/{}/lz4/{}".format(sdir,
                                                             self.name, subdir, k), v)
        if callable(self.zstd_log):
            data = read_file("{}/../out/zstd".format(sdir))
            out = self.zstd_log(data, config=config, status=self.status)
            for k, v in out.items():
                append_file("{}/../test/{}/{}/zstd/{}".format(sdir,
                                                              self.name, subdir, k), v)
        if callable(self.zero_log):
            data = read_file("{}/../out/zeros".format(sdir))
            out = self.zero_log(data, config=config, status=self.status)
            for k, v in out.items():
                append_file("{}/../test/{}/{}/zeros/{}".format(sdir,
                                                               self.name, subdir, k), v)

    def inc(self):
        self.status += 1
        write_file("{}/../test/{}/status".format(sdir,
                                                 self.name), str(self.status))
        if self.status >= len(self.configs):
            self.finished = True

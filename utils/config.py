
import json
import itertools
import os


class Config:
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
        f = open(
            "{}/config/redis-loader.json".format(os.environ['COMPRESSION_HOME']), 'w+')
        f.write(config_str)
        f.close()


def listify(x):
    return x if isinstance(x, list) else [x]


def write_status_file(path, val):
    try:
        f = open((path), 'w+')
        f.write(str(val))
    except Exception as e:
        print("ERR: failed to update status")
        print(str(e))
        exit(1)
    finally:
        f.close()


def read_status_file(path):
    try:
        f = open((path), 'r')
        ret = int(f.read())
    except Exception:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        f = open(path, 'w+')
        f.write("0")
        ret = 0
    finally:
        f.close()
    return ret


class ConfigIter:
    def __init__(self, recct=1, opct=1, fcount=1, fav=1, fdens=1.0, fdist='c', frange=1, fstd=1):
        params = list(map(listify, [recct, opct, fcount, fav, fdens]))
        params.append([])
        for dist in listify(fdist):
            if dist == 'c' or dist == 'p':
                params[-1].append((dist, 1, 1))
            if dist == 'u':
                for r in listify(frange):
                    params[-1].append(('u', r, 1))
            if dist == 'n':
                for s in listify(fstd):
                    params[-1].append(('n', 1, s))
        combinations = itertools.product(*params)
        self.config_list = list(map(lambda x: Config(
            x[0], x[1], x[2], x[3], x[4], x[5][0], x[5][1], x[5][2]), combinations))
        self.current = 0
        self.name = ""

    def bind(self, test_name):
        self.name = test_name
        self.current = read_status_file(
            "{}/test/{}/status".format(os.environ['COMPRESSION_HOME'], self.name))

    def __iter__(self):
        if self.name == "":
            print("ERR: need to bind instance to a test")
            exit(1)
        return self

    def __next__(self):
        if self.current >= len(self.config_list):
            raise StopIteration
        return self.config_list[self.current]

    def inc(self):
        self.current += 1
        write_status_file(
            "{}/test/{}/status".format(os.environ['COMPRESSION_HOME'], self.name), self.current)

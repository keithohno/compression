
import json
import itertools
import os


sdir = os.path.dirname(os.path.realpath(__file__))


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
        path = "{}/../config/redis-loader.json".format(sdir)
        try:
            f = open(path, 'w+')
        except Exception:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            f = open(path, 'w+')
        finally:
            f.write(config_str)
            f.close()

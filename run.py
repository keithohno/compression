
import subprocess
import json
import time
import random

CORE_NAME = "core"


class RecParams:
    def __init__(self, fcount=0, fdist='c', frange=10, fav=100, fdens=1.0, fstd=10):
        self.fcount = fcount
        self.fdist = fdist
        self.frange = frange
        self.fav = fav
        self.fdens = fdens
        self.fstd = fstd

    def __str__(self):
        if self.fdist == 'c':
            return "c{:03d}".format(self.fav)
        elif self.fdist == 'u':
            return "u{:03d}-{:03d}".format(self.fav, self.frange)
        elif self.fdist == 'n':
            return "n{:03d}-{:03d}".format(self.fav, self.fstd)
        else:
            return "p{:03d}".format(self.fav)


class WLParams:
    def __init__(self, recct=0, opct=0, rprms=RecParams()):
        self.recct = recct
        self.opct = opct
        self.rprms = rprms

    def to_str(self, recct=False, opct=False):
        return "{} {} {}".format(
            self.recct if recct else '',
            self.opct if opct else '',
            str(self.rprms))


def blockproc(proc):
    try:
        proc.wait()
        if proc.returncode != 0:
            print("exiting (subprocess error)...")
            print(proc.communicate()[0])
            exit()
    except KeyboardInterrupt:
        print("exiting (keyboard interrupt)...")
        proc.terminate()
        exit()


def workload(name):
    s = time.time()
    proc = subprocess.Popen(['./workload.sh', name],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    blockproc(proc)
    print("workload: {}s".format(time.time() - s))


def load_params(params):
    config = json.dumps({
        "record_count": params.recct,
        "operation_count": params.opct,
        "record_params": {
            "field_count": params.rprms.fcount,
            "field_dist": params.rprms.fdist,
            "field_range": params.rprms.frange,
            "field_av": params.rprms.fav,
            "field_density": params.rprms.fdens,
            "field_std": params.rprms.fstd
        }
    })
    f = open('redis-loader/config.json', 'w+')
    f.write(config)
    f.close()


def compress(name, folder, sizes, status, *args):
    s = time.time()
    proc = subprocess.Popen(['./zstd_compress.sh', name, folder] +
                            sizes, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    zstd_lines = proc.communicate()[0].decode('utf-8')
    blockproc(proc)
    print("zstd: {}s".format(time.time() - s))

    s = time.time()
    proc = subprocess.Popen(['./lz4_compress.sh', name, folder] +
                            sizes, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lz4_lines = proc.communicate()[0].decode('utf-8')
    blockproc(proc)
    print("lz4: {}s".format(time.time() - s))

    analyze(folder, lz4_lines, zstd_lines, status, *args)


def analyze(folder, lz4_lines, zstd_lines, status, *args):
    lz4_updates = lz4_analyze(lz4_lines)
    zstd_updates = zstd_analyze(zstd_lines)
    # lock status file
    proc = subprocess.Popen(['mkdir', "{}/status.lck".format(folder)])
    blockproc(proc)
    # update data files
    for item in lz4_updates:
        f = open("{}/lz4/{}".format(folder, item[0]), 'a+')
        f.write("{} {} {} {} {}\n".format(
            status, item[1], item[2], item[3], args[0]))
        f.close()
    for item in zstd_updates:
        f = open("{}/zstd/{}".format(folder, item[0]), 'a+')
        f.write("{} {} {} {} {}\n".format(
            status, item[1], item[2], item[3], args[0]))
        f.close()
    # update status file
    f = open("{}/status".format(folder), 'w+')
    f.write(str(status))
    f.close()
    # unlock status file
    proc = subprocess.Popen(['rm', '-r', "{}/status.lck".format(folder)])
    blockproc(proc)


def lz4_analyze(lines):
    lines = lines.split('SPLIT')
    orig = 0
    ret = []
    for line in lines:
        # skip initial empty line
        if len(line) == 0:
            continue
        # extract core size from first line
        words = line.split()
        if words[0] == CORE_NAME:
            orig = int(words[1])
            continue
        # for all other lines, output the following:
        # (comp block size, comp ratio, comp size)
        ratio = orig/int(words[2])
        ret.append((words[1], ratio, words[2], orig))
    return ret


def zstd_analyze(lines):
    lines = lines.split('SPLIT')
    orig = 0
    ret = []
    for line in lines:
        # skip initial empty line
        if len(line) == 0:
            continue
        # extract core size from first line
        words = line.split()
        if words[0] == CORE_NAME:
            orig = int(words[1])
            continue
        # calculate average compression ratio
        r_sum = 0
        r_ct = 0
        for item in line.split('(')[1:]:
            r_sum += float(item.split(')')[0])
            r_ct += 1
        r_sum /= r_ct
        # output:
        # (comp block size, comp ratio, comp size)
        ret.append((words[1], r_sum, int(orig/r_sum), orig))
    return ret


def get_status(folder):
    try:
        f = open("{}/status".format(folder), 'r')
        status = int(f.read())
        f.close()
        return status
    except FileNotFoundError:
        return -1


def clean(folder):
    proc = subprocess.Popen("rm -f {}/*.lz4".format(folder), shell=True)
    blockproc(proc)


def count_bytes(folder):
    s = time.time()
    proc = subprocess.Popen(['./count_zeros.sh', CORE_NAME, folder],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = proc.communicate()[0].decode(
        'utf-8').split('SPLIT')
    blockproc(proc)
    # o1 = zeros, o2 = size, o3 = not zeros, o4 = sequence breakdown
    o1 = output[1].strip()
    o2 = output[3].split('\t')[0].strip()
    o3 = int(o2) - int(o1)
    o4 = output[2].strip().splitlines()

    f = open("{}/zeros".format(folder), 'a+')
    f.write("{} {} {}\n".format(o1, o2, o3))
    f.close()

    f = open("{}/breakdown".format(folder), 'a+')
    for o in o4:
        f.write("{0: <10} ".format(o))
    f.write("\n")
    f.close()

    print("count: {}s".format(time.time() - s))


def test2():
    sizes = ['256', '512', '1K', '2K', '4K']
    base = 'test2'
    core = CORE_NAME

    test_set = [('c', 2, 1), ('u', 40, 2), ('u', 80, 2),
                ('u', 20, 2), ('u', 60, 2), ('n', 2, 10),
                ('n', 2, 20), ('n', 2, 30), ('p', 2, 2)]
    param_set = []

    for (fdist, frange, fstd) in test_set:
        param_set.append(WLParams(recct=4000000, opct=12000000,
                                  rprms=RecParams(fcount=5, fdist=fdist,
                                                  fav=100, frange=frange, fstd=fstd)))

    for i in range(len(param_set)):
        folder = "test/{}/{}".format(base, param_set[i].to_str().strip())
        clean(folder)
        status = get_status(folder)
        for j in range(status+1, 101, 1):
            s = time.time()
            param_set[i].rprms.fdens = j / 100
            load_params(param_set[i])
            workload(core)
            compress(core, folder, sizes, j, param_set[i].rprms.fdens)
            print("total time {}.{}: {}s".format(
                param_set[i].to_str(), j, time.time() - s))
            print("")


def test3():
    base = 'test3'
    core = CORE_NAME

    # test_set = [('c', 2, 1), ('u', 40, 2), ('u', 80, 2),
    #             ('u', 20, 2), ('u', 60, 2), ('n', 2, 10),
    #             ('n', 2, 20), ('n', 2, 30), ('p', 2, 2)]
    test_set = [('n', 2, 30), ('p', 2, 2)]
    param_set = []

    for (fdist, frange, fstd) in test_set:
        param_set.append(WLParams(recct=4000000, opct=12000000,
                                  rprms=RecParams(fcount=5, fdist=fdist,
                                                  fav=100, frange=frange, fstd=fstd)))

    for i in range(len(param_set)):
        folder = "test/{}/{}".format(base, param_set[i].to_str().strip())
        clean(folder)
        for j in range(0, 11, 1):
            s = time.time()
            param_set[i].rprms.fdens = j / 10
            load_params(param_set[i])
            workload(core)
            count_bytes(folder)
            print("total time {}.{}: {}s".format(
                param_set[i].to_str(), j, time.time() - s))
            print("")


if __name__ == '__main__':
    test3()

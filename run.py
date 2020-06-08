
import subprocess
import json
import time

CORE_NAME = "core"


class RecParams:
    def __init__(self, fcount=0, fdist='c', fcapmin=0, fcapmax=1, fcap=100, fdens=1.0):
        self.fcount = fcount
        self.fdist = fdist
        self.fcapmin = fcapmin
        self.fcapmax = fcapmax
        self.fcap = fcap
        self.fdens = fdens

    def to_str(self, fcount=False, fdist=False, fcapmin=False, fcapmax=False, fcap=False, fdens=False):
        return ("{} {} {} {} {} {}".format(
            self.fcount if fcount else '',
            self.fdist if fdist else '',
            self.fcapmin if fcapmin else '',
            self.fcapmax if fcapmax else '',
            self.fcap if fcap else '',
            self.fdens if fdens else ''))


class WLParams:
    def __init__(self, recct=0, opct=0, rprms=RecParams()):
        self.recct = recct
        self.opct = opct
        self.rprms = rprms

    def to_str(self, recct=False, opct=False, fcount=False, fcap=False, fdens=False):
        return("{} {} {}".format(
            self.recct if recct else '',
            self.opct if opct else '',
            self.rprms.to_str(fcount=fcount, fcap=fcap, fdens=fdens)))


def blockproc(proc):
    try:
        proc.wait()
        if proc.returncode != 0:
            print("exiting (subprocess error)...")
            exit()
    except KeyboardInterrupt:
        print("exiting (keyboard interrupt)...")
        proc.terminate()
        exit()


def workload(name):
    proc = subprocess.Popen(['./workload.sh', name])
    blockproc(proc)


def load_params(params):
    config = json.dumps({
        "record_count": params.recct,
        "operation_count": params.opct,
        "record_params": {
            "field_count": params.rprms.fcount,
            "field_cap_dist": params.rprms.fdist,
            "field_cap_min": params.rprms.fcapmin,
            "field_cap_max": params.rprms.fcapmax,
            "field_cap": params.rprms.fcap,
            "field_density": params.rprms.fdens,
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


def main():
    sizes = ['256', '512', '1K', '2K', '4K']
    base = 'test2'
    core = CORE_NAME

    test_set = [('c', 100, 101), ('u', 80, 121),
                ('u', 60, 141), ('u', 90, 111), ('u', 70, 131)]
    param_set = []

    for (fdist, fmin, fmax) in test_set:
        param_set.append(WLParams(recct=4000000, opct=12000000,
                                  rprms=RecParams(fcount=5, fdist=fdist,
                                                  fcap=100, fcapmin=fmin, fcapmax=fmax)))

    for i in range(len(param_set)):
        folder = "test/{}/{}{:03d}_{:03d}".format(
            base, param_set[i].rprms.fdist, param_set[i].rprms.fcapmin, param_set[i].rprms.fcapmax)
        clean(folder)
        status = get_status(folder)
        for j in range(status+1, 101, 1):
            s = time.time()
            param_set[i].rprms.fdens = j / 100
            load_params(param_set[i])
            workload(core)
            compress(core, folder, sizes, j, param_set[i].rprms.fdens)
            print("total time {}: {}s".format(j, time.time() - s))


if __name__ == '__main__':
    main()

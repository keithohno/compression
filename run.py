
import subprocess
import json

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
    proc = subprocess.Popen(['./lz4_compress.sh', name, folder] +
                            sizes, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lz4_lines = proc.communicate()[0].decode('utf-8')
    analyze(folder, lz4_lines, status, *args)
    blockproc(proc)


def analyze(folder, lz4_lines, status, *args):
    lz4_updates = lz4_analyze(lz4_lines)
    # lock status file
    proc = subprocess.Popen(['mkdir', "{}/status.lck".format(folder)])
    blockproc(proc)
    # update data files
    for item in lz4_updates:
        f = open("{}/lz4/{}".format(folder, item[0]), 'a+')
        f.write("{} {} {} {}\n".format(status, item[1], item[2], args[0]))
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
        ret.append((words[1], ratio, words[2]))
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
    base = 'test1'
    core = CORE_NAME
    recct = 4000000
    opct = 12000000
    for (fmin, fmax) in [(80, 120), (60, 140), (90, 110), (70, 130)]:
        folder = "test/{}/uni_cons{}-{}".format(base, fmin, fmax)
        clean(folder)
        status = get_status(folder)
        for i in range(status+1, 100, 1):
            rprms = RecParams(fcount=5, fdist='u', fcapmin=fmin,
                              fcapmax=fmax, fdens=i / 100)
            params = WLParams(recct=recct, opct=opct, rprms=rprms)
            load_params(params)
            workload(core)
            compress(core, folder, sizes, i, rprms.fdens)


if __name__ == '__main__':
    main()

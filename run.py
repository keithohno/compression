
import subprocess
import json


class RecParams:
    def __init__(self, fcount=0, fcap=100, fvolmin=0, fvolmax=400):
        self.fcount = fcount
        self.fcap = fcap
        self.fvolmin = fvolmin
        self.fvolmax = fvolmax

    def __str__(self):
        return ("{} {} {} {}".format(
            self.fcount,
            self.fcap,
            self.fvolmin,
            self.fvolmax))


class WLParams:
    def __init__(self, recct=0, opct=0, rprms=RecParams()):
        self.recct = recct
        self.opct = opct
        self.rprms = rprms

    def to_str(self, recct=False, opct=False, rprms=False):
        return("{} {} {}".format(
            self.recct if recct else '',
            self.opct if opct else '',
            str(self.rprms) if rprms else ''))


def blockproc(proc):
    try:
        proc.wait()
    except KeyboardInterrupt:
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
            "field_cap": params.rprms.fcap,
            "field_vol_min": params.rprms.fvolmin,
            "field_vol_max": params.rprms.fvolmax
        }
    })
    f = open('redis-loader/config.json', 'w+')
    f.write(config)
    f.close()


def compress(name, folder, sizes):
    proc = subprocess.Popen(['./compress.sh', name, folder] + sizes)
    blockproc(proc)


def analyze(name, folder, params):
    proc = subprocess.Popen(['./analyze.sh', name, folder],
                            stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    lines = out.decode('utf-8').splitlines()
    orig = int(lines[0].split()[0])
    for l in lines[1:]:
        ratio = round(int(l.split()[0]) / orig, 5)
        f = open("{}/{}".format(folder, l.split()[1].split('.')[0]), 'a+')
        f.write("{} {} {}\n".format(params.to_str(opct=True), 1/ratio, orig))
    blockproc(proc)


def prep(folder):
    proc = subprocess.Popen(["rm", "-rf", folder])
    blockproc(proc)


def cleanup(name, folder):
    proc = subprocess.Popen(['./cleanup.sh', name, folder])
    blockproc(proc)


def main():
    sizes = ['256', '512', '1K', '2K', '4K']
    base = 'test1'
    core = 'core'
    recct = 4000000
    for (fmin, fmax) in [(0, 1), (0, 26), (25, 51), (50, 76), (75, 101), (100, 101)]:
        folder = "test/{}/{:03d}-{:03d}".format(base, fmin, fmax - 1)
        prep(folder)
        for opct in range(0, 16000001, 500000):
            rprms = RecParams(fcount=5, fvolmin=fmin, fvolmax=fmax)
            params = WLParams(recct=recct, opct=opct, rprms=rprms)
            load_params(params)
            workload(core)
            compress(core, folder, sizes)
            analyze(core, folder, params)
            cleanup(core, folder)


if __name__ == '__main__':
    main()

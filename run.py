
import subprocess
import json


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
        f.write("{} {} {}\n".format(params.to_str(fdens=True), 1/ratio, orig))
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
    opct = 12000000
    for (fmin, fmax) in [(80, 120), (60, 140), (90, 110), (70, 130)]:
        folder = "test/{}/uni_cons{}-{}".format(base, fmin, fmax)
        prep(folder)
        for i in range(0, 100, 1):
            rprms = RecParams(fcount=5, fdist='u', fcapmin=fmin,
                              fcapmax=fmax, fdens=i / 100)
            params = WLParams(recct=recct, opct=opct, rprms=rprms)
            load_params(params)
            workload(core)
            compress(core, folder, sizes)
            analyze(core, folder, params)
            cleanup(core, folder)
        break


if __name__ == '__main__':
    main()

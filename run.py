
import subprocess
import json


class WLParams:
    def __init__(self, recct=0, opct=0):
        self.recct = recct
        self.opct = opct

    def to_str(self, recct=False, opct=False, rdprop=False, udprop=False):
        return("{} {}".format(
            self.recct if recct else '',
            self.opct if opct else ''))


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
        "operation_count": params.opct
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
        f.write("{} {} {}\n".format(params.to_str(opct=True), ratio, orig))
    blockproc(proc)


def prep(folder):
    proc = subprocess.Popen(["rm", "-rf", folder])
    blockproc(proc)


def cleanup(name, folder):
    proc = subprocess.Popen(['./cleanup.sh', name, folder])
    blockproc(proc)


def main():
    sizes = ['256', '512', '1K', '2K', '4K']
    base = 'DELETE_'
    core = 'core'
    for recct in range(20000, 40001, 20000):
        folder = "{}{:02d}".format(base, int(recct))
        prep(folder)
        for opct in range(20000, 40001, 20000):
            params = WLParams(recct=recct, opct=opct)
            load_params(params)
            workload(core)
            compress(core, folder, sizes)
            analyze(core, folder, params)
            cleanup(core, folder)


if __name__ == '__main__':
    main()

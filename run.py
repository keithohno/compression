
import subprocess

class WLParams:
    def __init__(self, recct=0, opct=0, rdprop=1, udprop=0):
        self.recct = recct
        self.opct = opct
        self.rdprop = rdprop
        self.udprop = udprop

    def to_str(self, recct=False, opct=False, rdprop=False, udprop=False):
        return("{} {} {} {}".format(\
                self.recct if recct else '', \
                self.opct if opct else '', \
                self.rdprop if rdprop else '', \
                self.udprop if udprop else ''))

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
    f = open('params.dat', 'w+')
    f.write("redis.host=127.0.0.1\n")
    f.write("redis.port=6379\n")
    f.write("recordcount={}\n".format(params.recct))
    f.write("operationcount={}\n".format(params.opct))
    f.write("readproportion={}\n".format(params.rdprop))
    f.write("updateproportion={}\n".format(params.udprop))
    f.close()

def compress(name, folder, sizes):
    proc = subprocess.Popen(['./compress.sh', name, folder] + sizes)
    blockproc(proc)

def analyze(name, folder, params):
    proc = subprocess.Popen(['./analyze.sh', name, folder], stdout=subprocess.PIPE)
    out, _ = proc.communicate()
    lines = out.decode('utf-8').splitlines()
    orig = int(lines[0].split()[0])
    for l in lines[1:]:
        ratio = round(int(l.split()[0]) / orig, 4)
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
    folder = 'test2'
    core = 'core'
    prep(folder)
    for i in range(0, 4000001, 25000):
        params = WLParams(recct=2000000, opct=i)
        load_params(params)
        workload(core)
        compress(core, folder, sizes)
        analyze(core, folder, params)
        cleanup(core, folder)

if __name__ == '__main__':
    main()

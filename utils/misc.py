
import time
import os

sdir = os.path.dirname(os.path.realpath(__file__))


class Stopwatch:
    def __init__(self):
        self.time = time.time()
        self.start = self.time

    def lap(self, message=""):
        print("{}{}s".format(message, round(time.time() - self.time, 3)))
        self.time = time.time()

    def total(self, message=""):
        print("{}{}s".format(message, round(time.time() - self.start, 3)))


def read_file(path):
    """Relative to compression root directory"""
    path = "{}/../{}".format(sdir, path)
    init_file(path)
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


def write_file(path, data):
    """Relative to compression root directory"""
    path = "{}/../{}".format(sdir, path)
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
    """Relative to compression root directory"""
    path = "{}/../{}".format(sdir, path)
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


def savefig(path, plt):
    path = "{}/../{}".format(sdir, path)
    init_dirs(path)
    plt.savefig(path)


def init_file(path):
    try:
        f = open((path), 'r')
    except Exception:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        f = open(path, 'w+')
    finally:
        f.close()


def init_dirs(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def init_status(path):
    status = read_file(path)
    if status == '':
        write_file(path, '0')
        return 0
    return int(status)


def extract_file(path):
    """Relative to compression root directory"""
    lines = read_file(path).splitlines()
    lines = [line.split() for line in lines]
    lines = [list(map(lambda x: int(x), line)) for line in lines]
    return lines


class Status:
    def __init__(self, test, max_stat):
        self.test = test
        self.current = init_status("res/{}/status".format(test))
        self.max_stat = max_stat
        self.finished = True if self.current >= self.max_stat else False

    def inc(self):
        self.current += 1
        write_file("res/{}/status".format(self.test), str(self.current))
        if self.current >= self.max_stat:
            self.finished = True

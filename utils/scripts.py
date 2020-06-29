
import subprocess
import os


def sdir(path=""):
    return "{}/{}".format(os.path.dirname(os.path.realpath(__file__)), path)


def block(proc):
    try:
        proc.wait()
        if proc.returncode != 0:
            print("ERR: subprocess failed")
            stdout, stderr = proc.communicate()
            if (stdout is not None):
                print(" - - -\nstdout:")
                print(stdout.decode('utf-8'))
            if (stderr is not None):
                print(" - - -\nstderr:")
                print(stderr.decode('utf-8'))
            exit()
    except KeyboardInterrupt:
        print("exiting (keyboard interrupt)")
        proc.terminate()
        exit()


def run_script(script, *, args=[], quiet=True):
    if quiet:
        proc = subprocess.Popen(
            [script] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        proc = subprocess.Popen([script] + args)
    block(proc)


def flush(quiet=True):
    run_script(sdir('bash/flush.sh'), quiet=quiet)


def redis_load(quiet=True):
    run_script(sdir('redis-loader/target/release/load'),
               args=[sdir('../config/redis-loader.json')], quiet=quiet)


def redis_run(quiet=True):
    run_script(sdir('redis-loader/target/release/run'),
               args=[sdir('../config/redis-loader.json')], quiet=quiet)


def redis_all(quiet=True):
    run_script(sdir('redis-loader/target/release/all'),
               args=[sdir('../config/redis-loader.json')], quiet=quiet)


def compress_zstd(quiet=True):
    run_script(sdir('compress/target/release/zstd'),
               args=[sdir('../out/core'), sdir('../out/zstd')], quiet=quiet)


def compress_lz4(quiet=True):
    run_script(sdir('compress/target/release/lz4'),
               args=[sdir('../out/core'), sdir('../out/lz4')], quiet=quiet)


def zero_count(quiet=True):
    run_script(sdir('zero-counter/target/release/zero-counter'),
               args=[sdir('../out/core'), sdir('../out/zeros')], quiet=quiet)


def dump(quiet=True):
    run_script(sdir('bash/dump.sh'), args=[sdir('../out')], quiet=quiet)


def build(quiet=True):
    run_script(sdir('bash/build.sh'),
               args=[sdir('redis-loader'), sdir('zero-counter'), sdir('compress')], quiet=quiet)

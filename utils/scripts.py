
import subprocess
import os


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


def run_script(script, quiet=True):
    script = "{}/{}".format(os.environ['COMPRESSION_HOME'], script)
    if quiet:
        proc = subprocess.Popen(
            script, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        proc = subprocess.Popen(
            script, stderr=subprocess.PIPE)
    block(proc)


def flush(quiet=True):
    run_script('utils/redis/flush.sh', quiet)


def redis_load(quiet=True):
    run_script('redis-loader/target/release/load', quiet)


def redis_run(quiet=True):
    run_script('redis-loader/target/release/run', quiet)


def redis_all(quiet=True):
    run_script('redis-loader/target/release/all', quiet)


def zero_count(quiet=True):
    run_script('zero-counter/target/release/zero-counter', quiet)


def dump(quiet=True):
    run_script('utils/redis/dump.sh', quiet)


def build(quiet=True):
    run_script('utils/build.sh', quiet)


import time


class Stopwatch:
    def __init__(self):
        self.time = time.time()

    def lap(self, message=""):
        print("{}{}s".format(message, round(time.time() - self.time, 3)))
        self.time = time.time()

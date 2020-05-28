import sys

def isolate_size(line):
    return int(line.split()[0])

def isolate_ratio(div):
    return lambda x: isolate_size(x) / div * 100

f = open(sys.argv[1], 'r')
lines = f.read().splitlines()
div = isolate_size(lines[0])
ratios = list(map(isolate_ratio(div), lines[1:]))

for r in ratios:
    print(r)

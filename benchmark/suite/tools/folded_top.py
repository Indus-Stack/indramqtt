#!/usr/bin/env python3
"""Summarise a folded stack file: self and inclusive share per function.
Usage: folded_top.py file.folded [N] [thread-name-filter]"""
import re
import sys
from collections import Counter

path = sys.argv[1]
top = int(sys.argv[2]) if len(sys.argv) > 2 else 25
tf = sys.argv[3] if len(sys.argv) > 3 else ""


def short(f):
    f = re.sub(r"<([^<>]*)>", lambda m: m.group(1), f) if len(f) > 90 else f
    return f[:110]


selfc, incl, threads = Counter(), Counter(), Counter()
total = 0
for line in open(path):
    line = line.rstrip("\n")
    if not line:
        continue
    stack, _, n = line.rpartition(" ")
    n = int(n)
    frames = stack.split(";")
    th = frames[0]
    threads[th] += n
    if tf and tf not in th:
        continue
    total += n
    fr = [short(x) for x in frames[1:]]
    if not fr:
        continue
    selfc[fr[-1]] += n
    for f in set(fr):
        incl[f] += n
print("total samples", total, "(all threads: %d)" % sum(threads.values()))
print("threads:", ", ".join("%s %.1f%%" % (k, 100.0 * v / sum(threads.values())) for k, v in threads.most_common(6)))
print("--- self")
for f, n in selfc.most_common(top):
    print("%5.1f%%  %s" % (100.0 * n / total, f))
print("--- inclusive")
for f, n in incl.most_common(top * 2):
    print("%5.1f%%  %s" % (100.0 * n / total, f))

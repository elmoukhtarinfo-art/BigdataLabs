#!/usr/bin/python3

import sys

# input comes from standard input STDIN
for line in sys.stdin:
    line = line.strip()          # remove leading/trailing spaces
    words = line.split()         # split into words
    for word in words:
        print(f"{word}\t1")      # key = word, value = 1

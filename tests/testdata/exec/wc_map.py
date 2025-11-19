#!/usr/bin/env python3
"""Word count mapper."""
import sys


for line in sys.stdin:
    words = line.split()
    # assert False, "FIXME: for each word, print the word and a '1'"
    for word in words:
        print(f"{word}\t1")
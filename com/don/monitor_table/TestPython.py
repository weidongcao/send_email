#!/usr/bin/python
# coding=utf-8
import sys


def get_sum(a, b):
    c = int(a) + int(b)
    print("c = " + str(c))

def get_string(bbb):
    bbb = "aaa" + bbb
    print(bbb)
    return bbb

# get_string(sys.argv[1])

if __name__ == "__main__":
    # get_sum(sys.argv[1], sys.argv[2])
    get_string(sys.argv[1])
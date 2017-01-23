#!/usr/bin/python
# coding=utf-8

import time, sys, os, datetime, re






aaa = [1, 3, 5, 7, 9, 2, 4, 6, 8, 10]

print("len(aaa) = " + str(len(aaa)))
bbb = aaa[0: aaa.__len__() - 2]
print("bbb = " + str(bbb))
print("index(bbb) = " + str(bbb[len(aaa) - 3]))

print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))

alist = [5L, 'ald_limao_receiver_agg', 'moblie_no', 'bigint', '', 'false', 'use', '2016-12-31 17:57:31', '2016-12-31 17:57:31']

print("list count : " + str(alist.count("string")))
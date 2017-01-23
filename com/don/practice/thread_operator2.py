#!/usr/bin/python
# coding=utf-8

import  datetime
num = 100
def func():
    aaa = num + 432
    print("num = " + aaa.__str__())
if __name__ == "__main__":
    # func()
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(now)

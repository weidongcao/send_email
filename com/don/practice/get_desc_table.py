#!/usr/bin/python
# coding=utf-8

import time, sys, datetime, re, PyMySQL, subprocess, shlex


def get_tables(file):
    tables = []

    content = open(file)
    lines = content.readlines()
    for table in lines:
        tables.append(table)

    return tables

def get_hive_table(sql):
    child = subprocess.Popen(["hive", "-e", sql], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    child.wait()
    print("--------------------")
    stdout, stderr = child.communicate()
    print ('stdout : '), stdout
    print ('stderr : '), stderr

    print("parent process")



# main function
if __name__ == '__main__':
    print("start: ----------------------")

    file = "D:\\bigdata\\resource\\TestData\\tables.txt"
    desc_format = 'desc %s.%s'
    database = 'leesdata'
    tables = get_tables(file)
    for table in tables:
        print(desc_format % (database, table))



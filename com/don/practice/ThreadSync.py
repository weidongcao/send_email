#!/usr/bin/python
# coding=utf-8

import time, sys, os, datetime, re, shlex

content = """
col_name      data_type       comment
mobile_raw              string
moblie_no               string
moblie_type             string
mobile_province         string
mobile_city             string
mobile_operators        string
receiver_name           string
tid_num                 int
ds                      string

# Partition Information
# col_name              data_type               comment

ds                      string
"""
def get_table_desc(stdout):
    lines = stdout.split("\n")
    dict = {}
    partition = []
    flat = False
    for index in range(len(lines)):
        line = lines[index]
        if line != '':
            if line.find("Partition Information") > 0:
                flat = True
                continue
            if (line.find("col_name") < 0) and (line.find("data_type") < 0):
                cols = re.split("\s+", line)
                if cols.__len__() == 2:
                    cols.append("")
                if flat == True:
                    cols.append("true")
                else:
                    cols.append("false")
                dict[cols[0]] = cols[1:]
    return dict


if __name__ == '__main__':
    infos = list(get_table_desc(content))
    for index in range(infos.__len__()):
        info = list(infos[index])
        print(info[0] + "\t\t", info[1] + "\t\t", info[2] + "\t\t", info[3] + "\t\t", info[4])
        # print(info[0])



    aaa = 'leesdata.idl_limao_address_raw_log'
    bbb = re.split("\.", aaa)
    print(re.split("\.", aaa)[1])
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
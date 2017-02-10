#!/usr/bin/python
# coding=utf-8

import time, sys, os, datetime, re


# get script file list
def file_list(directory, suffix):
    ## get file list
    files = os.listdir(directory)

    filelist = []
    for file in files:
        if ((os.path.isfile(directory + '/' + file) is True) and (file.endswith(suffix) == True)):
            ## add file
            filelist.append(file)
    return filelist


## replace ds date time
def replace_ds(file, p0):
    ## 打开文件
    content = open(file, 'r+')
    ## 读出所有行
    all_lines = content.readlines()
    ## 使用哪个数据库
    ## use_database = "use leesdata;"
    use_database = "use leesdata;"

    ## 相当于把游标重置为0
    content.seek(0)
    ## 把content清空
    content.truncate()
    ## 使用哪个数据库写入文件
    content.write(use_database)
    ## 换行
    content.write('\n')

    for line in all_lines:

        ## 将日期常量替换成变量
        ## line = replaceDate1(line, p0)

        ## 日期变量替换为常量
        line = replace_date_2(line, p0)

        ## 老的日期常量替换为新的日期常量
        ## line = replaceDate3(line, old_p0, new_p0)

        ## 如果原文件中包含了使用哪个数据库，不写入文件
        if ((line == (use_database + '\n'))
            or (line == (use_database + '\r\n'))):
            continue
        elif (line == ('use leestest;' + '\n')):
            continue
        else:
            if (line.find(use_database) >= 0):
                line = line.replace(use_database, '')
            content.write(line)

    content.close()


## 日期常量替换成变量
def replace_date_1(line, p0):
    p2 = get_date(p0, -1)
    p3 = get_date(p0, -5)

    ## 制表符替换为空格
    line = line.replace('\t', ' ')
    ## 双引号替换为单引号
    line = line.replace('"', '\'')

    line = line.replace(p0, '{p0}')
    line = line.replace(p2, '{p2}')
    line = line.replace(p3, '{p3}')
    return line


##日期变量替换为常量
def replace_date_2(line, p0):
    p2 = get_date(p0, -1)
    p3 = get_date(p0, -5)

    ## 制表符替换为空格
    line = line.replace('\t', ' ')
    ## 双引号替换为单引号
    line = line.replace('"', '\'')

    line = line.replace('{p0}', p0)
    line = line.replace('{p2}', p2)
    line = line.replace('{p3}', p3)
    return line


## 老的日期常量替换为新的日期常量
def replace_date_3(line, old_p0, new_p0):
    old_p2 = get_date(old_p0, -1)
    old_p3 = get_date(old_p0, -5)

    ## 制表符替换为空格
    line = line.replace('\t', ' ')
    ## 双引号替换为单引号
    line = line.replace('"', '\'')

    new_p2 = get_date(new_p0, -1)
    new_p3 = get_date(new_p0, -5)

    line = line.replace(old_p0, new_p0)
    line = line.replace(old_p2, new_p2)
    line = line.replace(old_p3, new_p3)
    return line


##根据给定的日期返回偏移的天数
def get_date(p0, offset):
    ## string 转date
    date_p0 = datetime.datetime.strptime(p0, '%Y-%m-%d')

    ## 日期偏移
    offset_date = date_p0 + datetime.timedelta(days=offset)

    ## 返回string类型的偏移日期
    return offset_date.strftime('%Y-%m-%d')


# 根据给定的秒返回分钟和小时
def get_run_time(runningtime):
    if runningtime < 60:
        return str(runningtime) + "second"
    elif (runningtime > 60) and (runningtime < (60 * 60)):
        return str(runningtime / 60) + "min " + str(runningtime % 60) + "second"
    elif (runningtime > (60 * 60)):
        return str(runningtime / (60 * 60)) + "hour " + str(runningtime / 60) + "min"


def get_time(stime):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stime))


def replace_files(directory, p0):
    suffix = '.sql'
    filelist = file_list(directory, suffix)

    for file in filelist:
        replace_ds(directory + '/' + file, p0)
        print(file + " replace done")


# 测试用
def test():
    sql = "ALTER TABLE leestest.idl_datasource_count_ft DROP PARTITION(tablename='odl_limao_order_logs',ds='2016-11-01'); insert into leestest.idl_datasource_count_ft partition(tablename, ds) select 'all_count' as colname, count(1) as data_count, 'odl_limao_order_logs' as tablename, ds from leestest.odl_limao_order_logs where ds = '2016-11-01' group by ds;"
    return sql
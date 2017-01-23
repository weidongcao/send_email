#!/usr/bin/python
# coding=utf-8

import datetime
import os
import time

from logger import logger
"""
获取监控脚本文件列表
参数说明:
directory:文件所在路径
suffix:文件后缀
"""
def file_list(directory, suffix):
    try:

        # get file list
        files = os.listdir(directory)
    except Exception as e:
        logger.error("e.message" + e.message)
        logger.error("could not read directory, please check the directory : " + directory)
        return False

    # 脚本文件名list
    filelist = []
    for sqlfile in files:
        if ((os.path.isfile(directory + '/' + sqlfile) is True) and (sqlfile.endswith(suffix) == True)):
            # add file
            filelist.append(sqlfile)
    logger.info("monitor script file list : " + ",".join(filelist))
    return filelist

"""
替换脚本的日期变量为指定常量
"""
def replace_ds(script_file, p0):
    ## 打开文件
    content = open(script_file, 'r+')

    ## 读出所有行
    all_lines = content.readlines()

    ## 使用哪个数据库
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

        ## 日期变量替换为常量
        line = replace_date_2(line, p0)

        ## 老的日期常量替换为新的日期常量
        ## line = replaceDate3(line, old_p0, new_p0)

        ## 如果原文件中包含了使用哪个数据库，不写入文件
        if ((line == (use_database + '\n')) or (line == (use_database + '\r\n'))):
            continue
        elif (line == ('use leestest;' + '\n')):
            continue
        else:
            if (line.find(use_database) >= 0):
                line = line.replace(use_database, '')
            content.write(line)

    content.close()


"""
将模板脚本中的日期常量替换成变量
"""
def replace_date_1(line, p0):
    p2 = get_offset_date(p0, -1)
    p3 = get_offset_date(p0, -5)

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
    p2 = get_offset_date(p0, -1)
    p3 = get_offset_date(p0, -5)

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
    old_p2 = get_offset_date(old_p0, -1)
    old_p3 = get_offset_date(old_p0, -5)

    ## 制表符替换为空格
    line = line.replace('\t', ' ')
    ## 双引号替换为单引号
    line = line.replace('"', '\'')

    new_p2 = get_offset_date(new_p0, -1)
    new_p3 = get_offset_date(new_p0, -5)

    line = line.replace(old_p0, new_p0)
    line = line.replace(old_p2, new_p2)
    line = line.replace(old_p3, new_p3)
    return line


##根据给定的日期返回偏移的天数
def get_offset_date(p0, offset):
    ## string 转date
    date_p0 = datetime.datetime.strptime(p0, '%Y-%m-%d')

    ## 日期偏移
    offset_date = date_p0 + datetime.timedelta(days=offset)

    ## 返回string类型的偏移日期
    return offset_date.strftime('%Y-%m-%d')

def get_time(stime):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stime))

def replace_files(directory, p0):
    suffix = '.sql'
    filelist = file_list(directory, suffix)
    
    for script_file in filelist:
        replace_ds(directory + '/' + script_file, p0)
        print(script_file + " replace done")

#测试用
def test():
    sql = "ALTER TABLE leestest.idl_datasource_count_ft DROP PARTITION(tablename='odl_limao_order_logs',ds='2016-11-01'); insert into leestest.idl_datasource_count_ft partition(tablename, ds) select 'all_count' as colname, count(1) as data_count, 'odl_limao_order_logs' as tablename, ds from leestest.odl_limao_order_logs where ds = '2016-11-01' group by ds;"
    return sql
if __name__ == "__main__":
    filelie = file_list("laskdjflaksjdf", ".sql")
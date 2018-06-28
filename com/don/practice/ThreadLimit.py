#!/usr/bin/python
# coding=utf-8

import datetime
import os
import threading
import time


# get script file list
def file_list(directory):
    # get file list
    files = os.listdir(directory)

    filelist = []
    for file in files:
        if ((os.path.isfile(directory + '/' + file) is True) and (file.endswith('.sql') == True)):
            # add file
            print('file = ' + file)
            filelist.append(file)
    return filelist;


# replace ds date time
def replace_ds(file, p0):
    # 打开文件
    content = open(file, 'r+')
    # 读出所有行
    all_lines = content.readlines()
    # 使用哪个数据库
    # use_database = "use leesdata;"
    use_database = "use leesdata;"

    # 相当于把游标重置为0
    content.seek(0)
    # 把content清空
    content.truncate()
    # 使用哪个数据库写入文件
    content.write(use_database)
    # 换行
    content.write('\n')

    for line in all_lines:

        # 将日期常量替换成变量
        # line = replaceDate1(line, p0)

        # 日期变量替换为常量
        line = replace_date_2(line, p0)

        # 老的日期常量替换为新的日期常量
        # line = replaceDate3(line, old_p0, new_p0)

        # 如果原文件中包含了使用哪个数据库，不写入文件
        if ((line == (use_database + '\n'))
            or (line == (use_database + '\r\n'))):
            print('if > ' + line)
            continue
        elif (line == ('use leestest;' + '\n')):
            continue
        else:
            if (line.find(use_database) >= 0):
                line = line.replace(use_database, '')
            content.write(line)

    content.close()


# 日期常量替换成变量
def replace_date_1(line, p0):
    p2 = get_date(p0, -1)
    p3 = get_date(p0, -5)

    # 制表符替换为空格
    line = line.replace('\t', ' ')
    # 双引号替换为单引号
    line = line.replace('"', '\'')

    line = line.replace(p0, '{p0}')
    line = line.replace(p2, '{p2}')
    line = line.replace(p3, '{p3}')
    return line


#日期变量替换为常量
def replace_date_2(line, p0):
    p2 = get_date(p0, -1)
    p3 = get_date(p0, -5)

    # 制表符替换为空格
    line = line.replace('\t', ' ')
    # 双引号替换为单引号
    line = line.replace('"', '\'')

    line = line.replace('{p0}', p0)
    line = line.replace('{p2}', p2)
    line = line.replace('{p3}', p3)
    return line


# 老的日期常量替换为新的日期常量
def replace_date_3(line, old_p0, new_p0):
    old_p2 = get_date(old_p0, -1)
    old_p3 = get_date(old_p0, -5)

    # 制表符替换为空格
    line = line.replace('\t', ' ')
    # 双引号替换为单引号
    line = line.replace('"', '\'')

    new_p2 = get_date(new_p0, -1)
    new_p3 = get_date(new_p0, -5)

    line = line.replace(old_p0, new_p0)
    line = line.replace(old_p2, new_p2)
    line = line.replace(old_p3, new_p3)
    return line


#根据给定的日期返回偏移的天数
def get_date(p0, offset):
    # string 转date
    date_p0 = datetime.datetime.strptime(p0, '%Y-%m-%d')

    # 日期偏移
    offset_date = date_p0 + datetime.timedelta(days=offset)

    # 返回string类型的偏移日期
    return offset_date.strftime('%Y-%m-%d')


def replace_files(directory, p0):
    filelist = file_list(directory)

    for file in filelist:
        replace_ds(directory + '/' + file, p0)
        print(file + " replace done")


def batch_run_hive(directory, p0):
    filelist = file_list(directory)

    sql = ''

    max_connect = 5
    semaphore = threading.Semaphore(max_connect)
    for file in filelist:
        content = open(directory + '/' + file)
        all_lines = content.readlines()
        for line in all_lines:
            # 将变量替换成日期常量
            line = replace_date_2(line, p0)
            sql += line

        content.close()
        while True:
            connect_num = threading.active_count()
            connect_list = threading.enumerate()

            for conn in connect_list:
                print("current running job : " + conn.getName())

            if connect_num < max_connect:
                threading.Thread(target=run_hive, name=(file + "_" + p0), args=(semaphore, sql)).start()
                break
            else:
                time.sleep(3)


def run_hive(semaphore, sql):
    semaphore.acquire()
    print("starting thread : " + threading.current_thread().getName())
    print(sql)
    time.sleep(5)
    os.system(
        'hive -e "' + sql + '" > /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')
    # os.system('echo ${JAVA_HOME} > /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')
    semaphore.release()


def print_sql(semaphore, sql):
    semaphore.acquire()
    counter = 8
    print("starting thread : " + threading.current_thread().getName())
    print(threading.current_thread().getName() + " = " + sql)
    while counter:
        print("%s : %s" % (threading.current_thread().getName(), time.ctime(time.time())))
        time.sleep(1)
        counter -= 1
    print("end thread : " + threading.current_thread().getName())
    semaphore.release()


def test():
    sql = "ALTER TABLE leestest.idl_datasource_count_ft DROP PARTITION(tablename='odl_limao_order_logs',ds='2016-11-01'); insert into leestest.idl_datasource_count_ft partition(tablename, ds) select 'all_count' as colname, count(1) as data_count, 'odl_limao_order_logs' as tablename, ds from leestest.odl_limao_order_logs where ds = '2016-11-01' group by ds;"
    print(sql)
    time.sleep(3)
    # os.system("hive -f '/data1/services/job_schedule/sql/monitor-test/ald_limao_receiver_agg.sql'")
    # os.system('hive -e "' + sql + '"')
    return sql


# main function
if __name__ == '__main__':
    # directory = "/data1/services/job_schedule/sql/daily-monitor"
    # directory = "/data1/services/job_schedule/sql/monitor-test"
    directory = "D:\\bigdata\\resource\\TestData\\script2016122620"
    # path = "/data1/services/job_schedule/sql/monitor-test"
    p0 = '2016-12-15'
    days = 1

    t = threading.Thread(target=test)
    # print("t.isAlive = " + t.is_alive)
    print(t.is_alive)
    time.sleep(4)
    # print("t.isAlive = " + t.is_alive)
    print(t.is_alive)

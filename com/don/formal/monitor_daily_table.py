#!/usr/bin/python
# coding=utf-8

import time, sys, os, datetime, re, threading
from utils import *


def batch_run_hive(directory, p0):
    # 需要监控的sql文件
    suffix = '.sql'
    filelist = file_list(directory, suffix)

    sql = ''

    # 最大并发数
    max_connect = 4
    # 设置多线程的最大并发数
    semaphore = threading.Semaphore(max_connect)

    # 跑需要监控的sql
    for i in range(len(filelist)):
        # sql 脚本文件
        file = filelist[i]

        # 打开sql脚本文件IO
        content = open(directory + '/' + file)
        all_lines = content.readlines()
        for line in all_lines:
            ## 将变量替换成日期常量
            line = replace_date_2(line, p0)
            sql += line

        # 关闭IO流
        content.close()

        # 等待线程池中有可用的资源
        while True:
            # 当前活动的线程数
            connect_num = threading.active_count()
            print("connect_num : " + str(connect_num))

            # 当前活动的线程集合
            connect_list = threading.enumerate()

            print("remain job size : " + str(len(filelist) - i))
            time.sleep(1)

            # 打印正在执行的hive进程
            for conn in connect_list:
                # 不打印主进程(有问题先禁掉)
                # if conn != threading.main_thread():
                print("current running job : " + conn.getName())

            print("----------------------------------------------------")
            if connect_num < max_connect:
                threading.Thread(target=run_hive, name=(file + "_" + p0), args=(semaphore, sql)).start()

                # 清空sql脚本
                sql = ''
                break
            else:
                time.sleep(1)


def run_hive(semaphore, sql):
    # 获取资源
    semaphore.acquire()

    print("starting thread : " + threading.current_thread().getName())
    print(sql)

    # job开始时间
    hive_run_starttime = time.time()

    # job开始时间写入日志
    os.system('echo "start job time: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
        hive_run_starttime)) + '" > /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')

    # 换行
    # os.system('echo "  " >> /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')

    # 避免sql变量里的引号与外部的混淆
    sql = sql.replace("'", "\'")

    # job sql写入日志
    os.system(
        'echo "start job time: ' + sql + '" > /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')

    # 执行hive(运行的时候放开)
    os.system(
        'hive -e "' + sql + '" >> /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')

    # job 结束时间
    hive_run_end_time = time.time()
    # job 运行时间
    hive_running_time = hive_run_end_time - hive_run_starttime

    os.system(
        'echo "         " >> /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')
    # job 结束时间写入日志
    os.system('echo "end job time: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
        hive_run_end_time)) + '" >> /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')
    # job运行时间写入日志
    os.system('echo "job running time:' + get_time(
        hive_running_time) + '" >> /data1/services/job_schedule/logs/monitor_' + threading.current_thread().getName() + '.log')

    # 释放资源
    semaphore.release()


## main function
if __name__ == '__main__':

    # 监控目录
    directory = "/data1/services/job_schedule/sql/daily-monitor"
    ## directory = "/data1/services/job_schedule/sql/monitor-test"
    # directory = "/data1/services/job_schedule/sql/daily_test_2016122415/"

    # 跑批开始日期
    p0 = '2016-12-20'
    # 跑几天的数据
    days = 1

    print('sql directory location : ' + directory)

    # 记录开始时间
    starttime = time.time()

    # 开始跑任务
    ##跑几天的数据
    for index in range(0, days):
        # 所跑任务的日期
        day = get_date(p0, index)

        # 批量跑当天所有的任务
        batch_run_hive(directory, day)

    # 休眠1秒, 因为是多线程，其他线程会影响此处的打印
    time.sleep(1)
    # 结束时间
    endtime = time.time()

    # 换行
    print("")
    print("starttime " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime)))
    print("endtime : " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endtime)))

    runningtime = endtime - starttime
    print("total running time : " + get_run_time(runningtime) + " second")









#!/usr/bin/python
# coding=utf-8

import threading

from constant_util import *
from database_utils import *
from utils import *
from hive_util import run_hive_sql


# 未运行的job数
REMAIN_TASK_NUM = 0

def batch_run_hive(directory, p0):
    global REMAIN_TASK_NUM

    #需要监控的sql文件类型(以.sql结尾)
    suffix = '.sql'

    # 获取监控脚本
    filelist = file_list(directory, suffix)

    logger.info("obtain sql script file list success")

    # 获取需要监控的task个数
    REMAIN_TASK_NUM = filelist.__len__()

    # 判断是否有需要监控的task
    if REMAIN_TASK_NUM:
        # 最大并发数(包括主线程)
        max_connect = 2
        # 设置多线程的最大并发数
        semaphore = threading.Semaphore(max_connect)

        logger.info("current max threading is : " + str(max_connect))
        #跑需要监控的sql
        for i in range(filelist.__len__()):
            # sql 脚本文件
            sqlfile = filelist[i]

            # 读取hive脚本文件内容
            sql = ''
            try:
                # 打开sql脚本文件IO
                content = open(directory + '/' + sqlfile)
                all_lines = content.readlines()
                for line in all_lines:
                    ## 将变量替换成日期常量
                    line = replace_date_2(line, p0)
                    sql += line

                # 关闭IO流
                content.close()
            except Exception as e:
                logger.error("read hive sql file fail ,filename = " + str(sqlfile))
                logger.error("e.message = " + e.message)
                continue

            # 等待线程池中有可用的资源再创建新线程
            while True:
                #当前活动的线程数
                connect_num = threading.active_count()
                logger.info("connect_num : " + str(connect_num - 1))

                #当前活动的线程集合
                connect_list = threading.enumerate()

                # 记录剩余task的个数
                logger.info("remain job size : " + str(REMAIN_TASK_NUM))
                time.sleep(1)

                #记录正在执行的hive进程
                for conn in connect_list:
                    #不打印主进程(有问题先禁掉)
                    #if conn != threading.main_thread():
                    logger.info("current running job : " + conn.getName())

                if connect_num < max_connect:
                    thread_name = sqlfile.split(".")[0] + "_" + p0
                    threading.Thread(target=run_hive, name=thread_name, args=(semaphore, sql)).start()

                    logger.info("starting new thread : " + thread_name)
                    # 清空sql脚本
                    sql = ''
                    break
                else:
                    time.sleep(5)
    else:
        logger.info("there is no table to be monitor")


def run_hive(semaphore, sql):
    # 声明这个变量是全局变量
    global REMAIN_TASK_NUM
    #获取资源
    semaphore.acquire()

    logger.info("starting hive thread : " + threading.current_thread().getName())
    logger.info("hive sql : " + sql)

    # job开始时间
    hive_run_starttime = time.time()

    # 避免sql变量里的引号与外部的混淆
    sql = sql.replace("'", "\'")

    try:
        # 执行hive(运行的时候放开)
        # os.system('hive -e "' + sql + '"')
        logger.info("测试,暂时把hive的执行语句注释掉,待部署到生产上再打开")
    except Exception as e:
        logger.error("e.massage = " + e.message)
        logger.error("running hive sql fail , hive sql is : " + sql)

    #job 结束时间
    hive_run_end_time = time.time()
    #job 运行时间
    hive_running_time = hive_run_end_time - hive_run_starttime

    # 未运行完的task减1
    REMAIN_TASK_NUM = REMAIN_TASK_NUM - 1

    logger.info("job done : " + threading.current_thread().getName())
    logger.info("job running time:" + get_time(hive_running_time))

    #释放资源
    semaphore.release()

"""
参数说明：
directory:hive sql 脚本所在的目录
p0：任务开始的日期
offset：从任务开始的日期算起跑几天的数据
"""
def run_service(directory, p0, offset):

    logger.info("monitor label service start")

    for index in range(offset):
        # 获取偏移后的日期
        run_day = get_offset_date(p0, index)

        # 跑一天所有的task
        logger.info("monitor label day: " + run_day + " start")
        batch_run_hive(directory, run_day)
        logger.info("monitor label day: " + run_day + " end")

    print("sleep")
    logger.info("monitor label service end")

def schedule_run():
    p0 = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    offset = 1
    directory = "resource/daily-monitor/label"
    run_service(directory, p0, offset)

if __name__ == '__main__':
    # filepath = "resource/daily-monitor/label"
    # p0 = "2017-01-20"
    # offset = 1
    # run_service(filepath, p0, offset)
    schedule_run()
    print("----------------------end------------------------")





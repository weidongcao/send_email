#!/usr/bin/python
# coding=utf-8

import threading

from constant_util import *
from database_utils import *
from hive_util import run_hive_sql

# 未运行的job数
REMAIN_TASK_NUM = 0

# 多线程开启
# 线程队列
# thread_list = []

def get_target_list(data_list):

    convert_data_list = []
    for info_list in data_list:
        info_dict = {}
        """SELECT job_id, job_name, param, table_name, table_type, check_status FROM result_target_log where check_status = 0;"""
        # 任务ID
        info_dict["job_id"] = info_list[0]
        # 任务名称
        info_dict["job_name"] = info_list[1]
        # job日期
        info_dict["param"] = info_list[2]
        # 表名
        info_dict["table_name"] = info_list[3]
        # 表类型
        info_dict["table_type"] = info_list[4]
        # task状态
        info_dict["check_status"] = info_list[5]

        convert_data_list.append(info_dict)
    logger.info("convert data success")
    return convert_data_list

def run_monitor(data_list):
    # 多线程开启
    # global thread_list
    logger.info("monitor start")

    for data in data_list:
        # 对表类型进行判断
        table_type = data["table_type"]

        # 获取表类型（现在只有hive表，后期可能有HBase表）
        # 根据表信息执行hive命令统计表当天的数据
        if table_type == "hive01":
            # 根据查询数组生成hive sql
            hivesql = transform_format_string(HIVE_SELECT_COUNT_FORMAT, [data])[0]
            # hivesql_dict = run_hive_sql(hivesql)

            # test
            hivesql_dict = {'returncode': 0, 'stderr': 'which: no hbase in (/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/opt/aws/bin:/data1/services/flume16/bin:/data1/services/hadoop272/bin:/data1/services/hadoop272/sbin:/data1/services/hive2/bin:/data1/services/redis-3.2.3/bin:/home/caoweidong/.local/bin:/home/caoweidong/bin)\nSLF4J: Class path contains multiple SLF4J bindings.\nSLF4J: Found binding in [jar:file:/data1/services/hive2/lib/hive-jdbc-2.0.0-standalone.jar!/org/slf4j/impl/StaticLoggerBinder.class]\nSLF4J: Found binding in [jar:file:/data1/services/hive2/lib/log4j-slf4j-impl-2.4.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]\nSLF4J: Found binding in [jar:file:/data1/services/hive2/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]\nSLF4J: Found binding in [jar:file:/data1/services/hive2/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]\nSLF4J: Found binding in [jar:file:/data1/services/hive2/lib/spark-assembly-1.5.0-hadoop2.6.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]\nSLF4J: Found binding in [jar:file:/data1/services/hive2/lib/spark-examples-1.5.0-hadoop2.6.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]\nSLF4J: Found binding in [jar:file:/data1/services/hadoop272/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]\nSLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.\nSLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]\n\nLogging initialized using configuration in jar:file:/data1/services/hive2/lib/hive-common-2.0.0.jar!/hive-log4j2.properties\nOK\nTime taken: 0.836 seconds\nWARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.\nQuery ID = caoweidong_20170113194045_8217c39b-4975-4641-9cad-a358d1d2872d\nTotal jobs = 2\nLaunching Job 1 out of 2\nNumber of reduce tasks not specified. Estimated from input data size: 1\nIn order to change the average load for a reducer (in bytes):\n  set hive.exec.reducers.bytes.per.reducer=<number>\nIn order to limit the maximum number of reducers:\n  set hive.exec.reducers.max=<number>\nIn order to set a constant number of reducers:\n  set mapreduce.job.reduces=<number>\nStarting Job = job_1482829985518_12045, Tracking URL = http://172.31.9.255:8038/proxy/application_1482829985518_12045/\nKill Command = /data1/services/hadoop272/bin/hadoop job  -kill job_1482829985518_12045\nHadoop job information for Stage-1: number of mappers: 1; number of reducers: 1\n2017-01-13 19:40:55,191 Stage-1 map = 0%,  reduce = 0%\n2017-01-13 19:40:59,383 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.97 sec\n2017-01-13 19:41:04,565 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 1.97 sec\nMapReduce Total cumulative CPU time: 1 seconds 970 msec\nEnded Job = job_1482829985518_12045\nLaunching Job 2 out of 2\nNumber of reduce tasks not specified. Estimated from input data size: 1\nIn order to change the average load for a reducer (in bytes):\n  set hive.exec.reducers.bytes.per.reducer=<number>\nIn order to limit the maximum number of reducers:\n  set hive.exec.reducers.max=<number>\nIn order to set a constant number of reducers:\n  set mapreduce.job.reduces=<number>\nStarting Job = job_1482829985518_12046, Tracking URL = http://172.31.9.255:8038/proxy/application_1482829985518_12046/\nKill Command = /data1/services/hadoop272/bin/hadoop job  -kill job_1482829985518_12046\nHadoop job information for Stage-2: number of mappers: 1; number of reducers: 1\n2017-01-13 19:41:12,884 Stage-2 map = 0%,  reduce = 0%\n2017-01-13 19:41:18,041 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 1.06 sec\n2017-01-13 19:41:24,228 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 2.96 sec\nMapReduce Total cumulative CPU time: 2 seconds 960 msec\nEnded Job = job_1482829985518_12046\nMapReduce Jobs Launched: \nStage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.62 sec   HDFS Read: 9789 HDFS Write: 125 SUCCESS\nStage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 2.96 sec   HDFS Read: 5874 HDFS Write: 23 SUCCESS\nTotal MapReduce CPU Time Spent: 6 seconds 580 msec\nOK\nTime taken: 40.264 seconds, Fetched: 1 row(s)\n', 'stdout': 'c0\n10\n'}

            # 处理从hive返回的数据
            if hivesql_dict:

                try:
                    # 取得hive查询返回结果
                    hive_select_return = hivesql_dict["stdout"].split("\n")[1]
                except Exception as e:
                    logger.error(e.message)
                    if hivesql_dict["stdout"]:
                        logger.error("acquire hive select result set fail, result set is : none")
                    else:
                        logger.error("acquire hive select result set fail, result set is :" + str(hivesql_dict["stdout"]))

                    # 结果重置
                    data["result"] = "null"
                    # 检查状态设为-1
                    data["check_status"] = -1
                    # 生成更新Mysql数据库的sql
                    sql_monitor_data_update = transform_format_string(MONITOR_DATA_UPDATE_FORMAT, [data])
                    # 根据job_id和table_name更新result_target_log表result字段，check_status字段，updatedt字段
                    batch_modify_database(sql_monitor_data_update)
                    # 跳过下面的继续下一次循环
                    continue

                # 查询结果可能空, 然后什么都不返回
                if hive_select_return:
                    data["result"] = hive_select_return
                else:
                    data["result"] = 0

                # 检查
                data["check_status"] = 2

                # 生成更新Mysql数据库的sql
                sql_monitor_data_update = transform_format_string(MONITOR_DATA_UPDATE_FORMAT, [data])
                # sql_monitor_data_update = ['updatedsd result_target_log set result = 10, updatedt = now(), check_status = 2 where job_id = "T7795753989178458" and table_name = "idl_address_pois_log";''update result_target_log set result = 10, updatedt = now(), check_status = 2 where job_id = "T7795753989178458" and table_name = "idl_address_pois_log";']

                # 根据job_id和table_name更新result_target_log表result字段，check_status字段，updatedt字段
                commit_result = batch_modify_database(sql_monitor_data_update)

                # 判断是否更新成功,如果没有更新成功则设为失败
                if commit_result:
                    pass
                else:
                    # 结果重置
                    data["result"] = "null"
                    # 检查状态设为-1
                    data["check_status"] = -1
                    # 生成更新Mysql数据库的sql
                    sql_monitor_data_update = transform_format_string(MONITOR_DATA_UPDATE_FORMAT, [data])
                    # 根据job_id和table_name更新result_target_log表result字段，check_status字段，updatedt字段
                    batch_modify_database(sql_monitor_data_update)
        logger.info("monitor success")

    """多线程开启"""
    # 将此线程从正在运行的线程队列中移除
    # t = threading.currentThread()
    # thread_list.remove(t)


def thread_monitor():

    # 多线程开启
    # global thread_list
    # threadcounter = 0

    flat = True
    while flat:
        # 查询需要监控的数据
        select_list = select_multi_data(MONITOR_DATA_SELECT_FORMAT)

        select_run_total_count = select_single_data(MONITOR_DATA_SELECT_COUNT_FORMAT)[0]

        # 有需要监控的数据进行监控，没有需要监控的数据结束监控
        if select_list and (select_run_total_count < 2):
            # 将数据记录为内部是字典的数组
            data_list = get_target_list(select_list)
            # 将Mysql里的数据标记为1表示现在在下处理
            for data in data_list:
                # 结果重置
                data["result"] = "null"
                # 检查状态设为1
                data["check_status"] = 1
                # 生成更新Mysql数据库的sql

            # 生成sql
            sql_monitor_data_update = transform_format_string(MONITOR_DATA_UPDATE_FORMAT, data_list)
            # 更新Mysql数据库里的数据为正在处理的状态
            batch_modify_database(sql_monitor_data_update)

            # 跑监控
            run_monitor(data_list)

            break
            # 正在跑的线程数
            # threadcounter += 1

            # 线程名
            # threadname = "monitor_thread_" + str(threadcounter)


            # 创建线程
            # t = threading.Thread(target=run_monitor, args=[data_list, ], name=threadname)
            # # 启动线程
            # t.start()

            # 加入正在运行的线程队列
            # thread_list.append(t)

            # 检查当前在跑的线程如果超过或者等于最大线程数等待
            # max_thread_num = 2
            # while True:
            #     if thread_list.__len__() >= max_thread_num:
            #         print("current thread is outnumber, wait to acquire resource")
            #         time.sleep(5)
            #     else:
            #         print("acquired resource, start new thread")
            #         break
        else:
            # 如果还有正在跑的线程,等等跑完主线程再结束
            # while True:
            #     if thread_list.__len__() > 0:
            #         print("remain running thread is : " + str(thread_list.__len__()))
            #         time.sleep(5)
            #     else:
            #         # 如果没有需要监控的task结束循环
            #         break
            print("select_run_total_count = " + str(select_run_total_count))
            flat = False



def main():
    thread_monitor()
    # select_run_total_count = select_single_data(MONITOR_DATA_SELECT_COUNT_FORMAT)[0]
    # print("select_run_total_count = " + str(select_run_total_count))
    print("sleep")

if __name__ == '__main__':
    main()


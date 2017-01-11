#!/usr/bin/python
# -*- coding: utf8 -*-

import MySQLdb
import time

from constant_util import *
from json_util import *
from logger import logger

"""文件存放数据库操作相关函数"""

# 获取数据库连接信息
def get_database(conf_path, type_name, conn_name):
    poor = get_json(conf_path)
    conn_info = poor[type_name][conn_name]
    if conn_info:
        return conn_info
    else:
        logger.error("获取数据库连接信息失败")
        logger.error("error connection info : conf_path ＝ " + conf_path + " type_name ＝　" + type_name + "; conn_name = " + conn_name)
        return False


# 获取Mysql数据库连接
def get_connect():
    # conn_info = get_database("D:\\bigdata\\workspace\\PycharmProjects\\python\\com\\don\\formal\\resource\\application_content.json", "mysql_database", "email_service")
    conn_info = get_database("D:\\bigdata\\workspace\\PycharmProjects\\python\\com\\don\\formal\\resource\\application_content.json", "mysql_database", "localhost")

    if conn_info:
        hostname = conn_info['hostname']
        hostport = conn_info['hostport']
        username = conn_info['username']
        password = conn_info['password']
        database = conn_info['database']
        charset  = conn_info['charset']
        try:
            conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database, port=hostport, charset=charset)
            logger.info("database connection success")
            return conn
        except Exception as e:
            logger.debug("数据库连接失败 : " + e.message)
            return False

    else:
        logger.debug("获取数据库连接信息失败")
        return False


def get_error_job():
    conn = get_connect()
    cursor = conn.cursor()
    cursor.execute(EMAIL_SELECT_ERROR_JOB)
    rows = cursor.fetchall()

    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()
    return rows

def select_multi_data(sql):
    conn = get_connect()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    except Exception as e:
        logger.error(e.message)
        logger.error("select error, sql is " + sql)
        rows = False
    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()
    return rows

def select_single_data(sql):
    conn = get_connect()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
    except Exception as e:
        logger.error(e.message)
        logger.error("select error, sql is " + sql)
        row = False

    # for row in rows:
    #     print(str(row[0]) + "\t\t" + row[1] + row[2] + "\t\t" + str(row[3]) + "\t\t" + str(row[4]) + "\t\t" + str(row[5]))
    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()
    return row

def batch_modify_database(sql_list):
    if sql_list:
        #获取连接
        conn = get_connect()
        #获取连接游标
        cursor = conn.cursor()
        # 批量更新(插入)
        for sql in sql_list:
            try:
                cursor.execute(sql)
                # 提交事务
                conn.commit()
                flat = True
            except Exception as e:
                logger.error(e.message)
                logger.error("execute sql fail : sql = " + sql)
                flat = False
                break
    else:
        logger.error("parameter is none")
        flat = False

    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()
    return flat

#替换模板:将｛key｝替换为value
def transform_temple_content(temple, data_list):
    if temple:
        if data_list:
            info_list = []
            for data in data_list:
                info = temple
                for key in data:
                    try:
                        info = info.replace('{' + key + '}', str(data[key]))
                    except Exception as e:
                        logger.error(e.message)
                        logger.error("convert error : key = " + key + "; value = " + str(data[key]))
                        info_list = False
                        break
                info_list.append(info)
        else:
            logger.error("data_list is none")
            info_list = False
    else:
        logger.error("temple is none")
        info_list = False
    return info_list

# 测试
def test(sql):
    conn = get_connect()
    cursor = conn.cursor()
    # cursor.execute(SELECT_FORMAT %(TABLENAME, "idl_limao_address_raw_log"))

    cursor.execute(sql)

    row = cursor.fetchone()
    # for row in rows:
    print(str(row[0]) + "\t\t" + str(row[1]) + str(row[2]) + "\t\t" + str(row[3]) + "\t\t" + str(row[4]))
    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()

def get_time(stime):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stime))

# 执行存储过程
def call_proc(proc_name):
    conn = get_connect()
    cursor = conn.cursor()
    try:
        cursor.callproc(proc_name)
        conn.commit()
        flat = True
    except Exception as e:
        logger.error(e.message)
        logger.error("call procedure fail : " + proc_name)
        flat = False

    cursor.close()
    conn.close()
    return flat


if __name__ == '__main__':
    # get_error_job()
    # sql = EMAIL_SELECT_CONf_FOTMAT.replace('{conf_name}', 'error_report')
    # sql = "select * from config_email_info where email_idd = 13"
    # row = select_multi_data(sql)
    #
    # if row:
    #     print(str(row[0]) + "\t\t" + str(row[1]) + str(row[2]) + "\t\t" + str(row[3]) + "\t\t" + str(row[4]))
    # else:
    #     print(row)

    stli = ["ad"]
    if stli:
        print(True)
    else:
        print(False)
    # call_proc("proc_main")
    # curdate = time.strftime("%Y-%m-%d-%H-%M", time.localtime(time.time()))
    # print(curdate)
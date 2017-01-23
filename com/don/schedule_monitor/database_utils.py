#!/usr/bin/python
# -*- coding: utf8 -*-

import MySQLdb
import time

from json_util import *
from logger import logger

"""文件存放数据库操作相关函数"""

"""
获取数据库连接信息
包括主机名、主机端口、用户名、密码、数据库、字符编码
"""
def get_database(conf_path, type_name, conn_name):
    poor = get_json(conf_path)
    conn_info = poor[type_name][conn_name]
    if conn_info:
        return conn_info
    else:
        logger.error("获取数据库连接信息失败")
        logger.error("error connection info : conf_path ＝ " + conf_path + " type_name ＝　" + type_name + "; conn_name = " + conn_name)
        return False


"""
根据数据库连接信息获取Mysql数据库连接
"""
def get_connect():
    conn_info = get_database("resource/application_context.json", "mysql_database", "control_job_dev")
    # conn_info = get_database("resource/application_context.json", "mysql_database", "datacenter")
    # conn_info = get_database("D:\\bigdata\\workspace\\PycharmProjects\\python\\com\\don\\formal\\resource\\application_content.json", "mysql_database", "localhost")

    if conn_info:
        hostname = conn_info['hostname']
        hostport = conn_info['hostport']
        username = conn_info['username']
        password = conn_info['password']
        database = conn_info['database']
        charset  = conn_info['charset']
        try:
            # conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database, port=hostport, charset=charset)
            conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database, port=hostport)
            logger.info("database connection success")
            return conn
        except Exception as e:
            logger.debug("数据库连接失败 : " + e.message)
            return False

    else:
        logger.debug("获取数据库连接信息失败")
        return False

"""
一次查询多条记录,
返回查询的所有数据
"""
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
    logger.info("select multi-data success")
    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()
    return rows

"""
一个查询一条记录
返回一条数据
"""
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

    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()
    return row

"""
批量更新数据库,可以是sql可以是插入,修改,删除
返回修改结果
如果修改成功返回True,如果修改失败返回False
"""
def batch_modify_database(sql_list):
    #获取连接
    conn = get_connect()
    #获取连接游标
    cursor = conn.cursor()
    if sql_list:
        # 批量更新(插入)
        for dbsql in sql_list:
            try:
                cursor.execute(dbsql)
                # 提交事务
                conn.commit()
                flat = True
            except Exception as e:
                logger.error(e.message)
                logger.error("execute sql fail : sql = " + dbsql)
                conn.rollback()
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

"""
替换模板:将｛key｝替换为value
此方法主要是替换constant_*_util.py里的常量
"""
def transform_format_string(temple, data_list):
    #判断模板是否为空
    if temple:
        # 判断数据是否为空
        if data_list:
            info_list = []

            # 对list里的数据进行循环替换
            for data in list(data_list):
                info = temple
                for key in data:
                    try:
                        # 根据key替换成value
                        info = info.replace('{' + key + '}', str(data[key]))
                    except Exception as e:
                        logger.error(e.message)
                        logger.error("convert error : key = " + key + "; value = " + str(data[key]))
                        return False
                info_list.append(info)
        else:
            logger.error("data_list is none")
            info_list = False
    else:
        logger.error("temple is none")
        info_list = False

    if info_list:
        logger.info("transform string format success")
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

"""
执行存储过程
"""
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
  sql = "select *from result_target_log where job_id = 'T7795753989178458'"
  test(sql)


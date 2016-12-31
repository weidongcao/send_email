#!/usr/bin/python
# -*- coding: utf8 -*-

import MySQLdb, time
from constant_util import *

"""文件存放数据库操作相关函数"""

# 获取Mysql数据库连接
def get_connect():
    # 主机名
    hostname = '54.223.85.243'
    # 用户名
    username = 'dc'
    # 密码
    password = 'mCdlUmm3thna5ttup'
    #要连接的数据库实例
    database = 'datacenter'
    # 端口号
    port = 3306

    #进行连接
    conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database, port=port)

    return conn

# INSERT_FORMAT = 'insert into "%s"(tid, tablename, colname, coltype, tcomment, is_partition_field, colstatus, create_date, update_date) values (null, "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s")'
# 批量插入数据到Mysql
def insert_infos(insert_format, datas):
    #获取连接
    conn = get_connect()

    #获取连接游标
    cursor = conn.cursor()

    # 批量插入
    for info in datas:
        cursor.execute(insert_format % (TABLENAME, info[0], info[1], info[2], info[3], info[4], info[5], get_time(time.time()), get_time(time.time())))

    # 提交事务
    conn.commit()

    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()

# UPDATE_FORMAT = "update %s set tablename = '%s', colname = '%s', coltype = '%s', tcomment = '%s', is_partition_field = '%s', colstatus = '%s', update_date = '%s' where tid = '%s'"
def update_infos(update_format, datas):
    #获取连接
    conn = get_connect()
    #获取连接游标
    cursor = conn.cursor()
    # 批量更新
    for info in datas:
        cursor.execute(update_format % (TABLENAME, info[1], info[2], info[3], info[4], info[5], info[6], get_time(time.time()), info[0]))

    # 提交事务
    conn.commit()

    # 关闭游标
    cursor.close()
    # 断开连接
    conn.close()

# 查询表信息
def get_table_infos(sql):
    conn = get_connect()
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

# 测试
def test():
    conn = get_connect()
    cursor = conn.cursor()
    # cursor.execute(SELECT_FORMAT %(TABLENAME, "idl_limao_address_raw_log"))

    sql = "select * from table_dictionary where tablename = 'ald_limao_receiver_agg'"
    cursor.execute(sql)

    rows = cursor.fetchall()
    for row in rows:
        print(str(row[0]) + "\t\t" + row[1] + row[2] + "\t\t" + row[3] + "\t\t" + row[4] + "\t\t" + row[5] + "\t\t" + row[6] + "\t\t" + row[7])

def get_time(stime):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stime))

if __name__ =='__main__':
    test()

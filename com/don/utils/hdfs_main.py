#!/usr/bin/python
# -*- coding: utf8 -*-

import datetime
import os
import sys

import shutil

from hdfs_util import exec_sh
from hdfs_util import get_unzip_hdfs_file_from_dir
from logger import logger
from utils import get_date

reload(sys)
sys.setdefaultencoding("utf-8")

root_dir = ''


# 获取脚本文件的当前路径
def cur_file_dir():
    # 获取脚本路径
    root_path = sys.path[0]
    # 判断为脚本文件还是py2exe编译后的文件，如果是脚本文件，则返回的是脚本的目录，如果是py2exe编译后的文件，则返回的是编译后的文件路径
    if os.path.isdir(root_path):
        return root_path
    elif os.path.isfile(root_path):
        return os.path.dirname(root_path)


"""
根据shell命令切分文件
参数说明:
    cmd:
        shell 切分命令
    base_path:
        根目录
"""


def split_file(cmd, base_path):
    split_result = exec_sh(cmd, base_path)
    return split_result


"""
切分指定目录下的文件
参数说明:
    file_list:
        需要切分的文件列表
    save_dir:
        本地保存目录
    line_size:
        切分大小（行）
"""


def split_file_from_dir(file_list, save_dir, line_size):
    # 文件切分命令模板
    SPLIT_CMD_TEMPLE = "split -l {LINE_SIZE} {full_file_name} -d -a 4 {local_file};"

    # 替换要切分的行数
    split_linesize_cmd = SPLIT_CMD_TEMPLE.replace("{LINE_SIZE}", str(line_size))

    print("file_list = " + file_list.__str__())
    # 切分文件列表下的所有文件
    for file in file_list:
        # 要切分的文件名
        filename = os.path.basename(file)
        # 要切分的文件路径
        filepath = os.path.dirname(file)

        # 替换shell命令中要切分的文件
        split_cmd = split_linesize_cmd.replace("{full_file_name}", file)

        # shell命令中切分后的文件名（没有下标,如文件名为aaa00001,就是aaa）
        local_file = save_dir + "/" + filename + "_"

        # 替换shell命令中切分后的文件主（没有下载）
        split_cmd = split_cmd.replace("{local_file}", local_file)
        logger.info("split cammond: " + split_cmd)

        # 文件切分
        split_file(split_cmd, filepath)


"""
逻辑说明:
    参数校验
    以下操作循环操作:
        下载HDFS文件
        解压缩文件
        删除原文件（本地下载的未解压的文件）
        文件切分
参数说明:
    table_list:
        hive表名的列表
    partition_field:
        分区字段名
    p0:
        分区字段值
    days:
        分区天数（以p0开始的天数）
    save_local_dir:
        本地保存目录
"""


def main(table_list, partition_field, p0, days, save_local_dir):
    global root_dir
    # 参数校验
    # 校验表列表
    if not table_list:
        logger.error("表名的列表为空")
        return

    # 校验天数
    try:
        days = int(days)
    except Exception as e:
        logger.error("p0日期格式不正确")
        return

    # 校验保存目录
    if not os.path.isdir(save_local_dir):
        logger.error("指定本地目录不存在")
        return

    # 校验日期
    try:
        datetime.datetime.strptime(p0, '%Y-%m-%d')
    except Exception as e:
        logger.error("p0日期格式不正确")
        return

    # 第一个切分后的文件有多少行
    LINE_SIZE = 50000
    # LINE_SIZE = 5000
    # 文件操作目录
    hdfs_file_operator_dir = "hdfs_file_temp"
    hdfs_file_operator_dir = root_dir + "/" + hdfs_file_operator_dir

    # hdfs 路径模板
    HDFS_PATH_TEMPLE = "/user/hive/warehouse/leesdata.db/{tablename}/{partition_field}={p0}/"
    # HDFS_PATH_TEMPLE = "/user/hive/warehouse/leestest.db/{tablename}/{partition_field}={p0}/"

    # 循环操作给定的表
    for tablename in table_list:
        # hdfs 路径模板替换表名
        hdfs_path_table = HDFS_PATH_TEMPLE.replace("{tablename}", tablename)
        hdfs_path_table = hdfs_path_table.replace("{partition_field}", partition_field)

        # 本地路径添加表名目录
        hdfs_file_table_dir = hdfs_file_operator_dir + "/" + tablename

        # 根据表名在临时目录下创建新目录
        try:
            # 如果存在此目录不存在创建此目录
            if not os.path.isdir(hdfs_file_table_dir):
                os.mkdir(hdfs_file_table_dir)
        except Exception as e:
            logger.error(e)
            logger.error("创建指定目录下表存储目录失败：" + hdfs_file_table_dir)
            return

        # 循环操作指定天数的数据文件
        for index in range(days):
            # 哪一天的数据
            ds = get_date(p0, index)

            # 当天数据文件所在的URL路径
            hdfs_path = hdfs_path_table.replace("{p0}", ds)
            logger.info("hdfs path url : " + hdfs_path)

            # 在临时目录根据表名创建的目录下，根据分区名创建新的目录
            hdfs_file_table_partition_dir = hdfs_file_table_dir + "/" + ds
            try:
                # 如果目录存在删除此目录
                if os.path.isdir(hdfs_file_table_partition_dir):
                    shutil.rmtree(hdfs_file_table_partition_dir)

                # 创建分区字段目录
                os.mkdir(hdfs_file_table_partition_dir)
            except Exception as e:
                logger.error(e)
                logger.error("创建表存储目录下的日期目录失败： " + hdfs_file_table_partition_dir)
                return

            # 数据文件下载并解压
            file_list = get_unzip_hdfs_file_from_dir(hdfs_path, hdfs_file_operator_dir)
            # 判断分区下是否有文件,有数据,对数据文件进行切分,没有数据跳过
            if file_list:
                logger.info("hdfs file download and uncompressed success")
                logger.info("uncompressed dir is : " + hdfs_file_operator_dir)

                # 文件切分
                split_file_from_dir(file_list, save_local_dir, LINE_SIZE)

                # 清空临时目录下下载的文件
                exec_sh("rm -rf " + hdfs_file_table_partition_dir, hdfs_file_table_dir)
                logger.info("split success table: " + tablename + " date: " + ds)
            else:
                logger.info("current partition has no data the table is : %s, partition : %s" % (tablename, ds))


if __name__ == "__main__":
    # result = get_unzip_hdfs_file("/user/000000_0.gz", "/home/caoweidong/data/")
    # print("get_unzip_hdfs_file = " + result)

    # global root_dir
    root_dir = cur_file_dir()

    args_list = sys.argv
    table_list = args_list[1].split(",")
    main(table_list, args_list[2], args_list[3], args_list[4], args_list[5])


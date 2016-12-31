#!/usr/bin/python
# coding=utf-8

import re, subprocess
from database_utils import *
from constant_util import *

# 根据文件获取文件内存放的表名
def get_tables(file):
    tables = []

    content = open(file)
    lines = content.readlines()
    for table in lines:
        tables.append(table)

    return tables


# 处理hive返回的数据,将之转换成一个字典返回
def get_table_desc(stdout):

    # 将原始数据以行分割
    lines = stdout.split("\n")

    # 要返回的字典
    dict = {}

    # 标识是否为分区
    flat = False

    #以行为单位处理原始数据
    for index in range(len(lines)):

        line = lines[index]

        # 原始数据中有空行
        if line != '':

            # 标识分区信息
            if line.find("Partition Information") > 0:
                flat = True
                continue

            # 过滤表头
            if (line.find("col_name") < 0) and (line.find("data_type") < 0):

                # 将数据以空字符切割为列表
                cols = re.split("\s+", line)

                # 如果字段注释为空,在列表中显示出来
                if cols.__len__() == 2:
                    cols.append("")

                # 如果此字段分区字段,标记为true, 否则为false
                if flat == True:
                    cols.append("true")
                else:
                    cols.append("false")

                # 以字段名为key,存储为字典
                dict[cols[0]] = cols
    return dict

# 处理hive表信息,看哪些信息是新增、修改、删除的，返回这些修改过的信息
def check_table_info(hive_tablename, stdout):

    # 处理hive原始输出信息
    dict = get_table_desc(stdout)

    # 查询Mysql数据库相关记录
    table_infos = get_table_infos(SELECT_FORMAT % (TABLENAME, hive_tablename))

    # 需要修改到数据库的信息
    infos = {}

    # 插入字段信息到数据库
    inst = []

    # 更新字段信息到数据库
    upda = []

    # 将从Mysql数据库查询到的信息以字典存储
    mdict = {}
    # 以字段名为key存储为字典
    for info in table_infos:
        mdict[info[2]] = info

    #将从hive获取到的数据与Mysql存储的数据进行比较
    if len(dict) > 0:
        for dcol in dict:

            # 从hive获取到的一条字段信息
            dval = list(dict[dcol])

            # Mysql中是否已经存储了这个表的信息，如果没有的话数据插入
            if len(mdict) > 0:

                # Mysql中是否已经存储了这个表的这个字段,如果没有数据插入,如果如果存在的话比较字段其他信息是否被修改
                if mdict.has_key(dcol):

                    # Mysql中存储的这个表这个字段的数据
                    mval = list(mdict[dcol])

                    if ((mval.count(dval[1]) == 0) or     #判断字段类型是否修改
                        (mval.count(dval[2]) == 0) or    #判断字段注释是否修改
                        (mval.count(dval[3]) == 0)) :          #字段是否为分区字段是否修改

                        # 将从hive处获取的字段信息加下表名,Mysql中数据主键, 并将状态设置为use（正在使用），
                        dval = add_col_info(dval, hive_tablename, "use", mval[0])

                        # 添加到数据更新的队列
                        upda.append(dval)
                else:
                    # Mysq中存在表数据,不存在此字段数据
                    dval = add_col_info(dval, hive_tablename, "use", None)

                    # 添加到数据插入的队列
                    inst.append(dval)
            else:
                # Mysq中不存在表数据
                dval = add_col_info(dval, hive_tablename, "use", None)

                inst.append(dval)

        # 检查Mysql数据库是存在的字段但是hive中不存在的字段（此情况出现于对表字段重命名）
        if len(mdict) > 0:
            for mcol in mdict:

                # Mysql中字段贪睡
                mval = list(mdict[mcol])

                # 如果hive中存在,跳过
                if dict.has_key(mcol):
                    continue
                # 如果hive中不存在,将Mysql中此字段状态设置为disabled(弃用状态)
                else:

                    # 截掉创建日期和更新日期
                    end_index = len(mval) - 2
                    tmp = mval[0: end_index]

                    # 字段状态标识为弃用
                    tmp[end_index - 1] = "disabled"

                    # 添加到数据更新的队列
                    upda.append(tmp)

    # 将需要插入到数据库的信息加入字典
    infos["inst"] = inst
    # 将需要更新到数据库的信息加入字典
    infos["upda"] = upda

    return infos

def add_col_info(info, tablename, colstatus, tid):

    # 列表反转
    info.reverse()

    # 添加表信息
    info.append(tablename)

    # 添加主键
    if tid is not None:
        info.append(tid)
    # 再反转回来
    info.reverse()

    # 添加字段状态
    info.append(colstatus)

    return info

# 获取原始的表信息
def get_hive_table(sql):

    # 执行Linux命令,通过hive -e连接到hive并执行命令
    run_hive = subprocess.Popen(["hive", "-e", sql], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # 等待子线程执行完毕
    run_hive.wait()

    # 获取原始输出
    stdout, stderr = run_hive.communicate()

    # 这是测试用,开发完后注释掉
    # stdout = DEMO_DESC_TABLE
    result_run_hive = run_hive.returncode

    return stdout


def main():
    # 需要监控的表名放在此文件里
    # file = "/home/caoweidong/python/formal/tables"
    file = "D:\\bigdata\\resource\\TestData\python\\tables.txt"

    # 获取需要监控的表名列表
    tables = get_tables(file)

    # 分析每一个需要监控的表，将表信息存放在mysql数据库
    for tablename in tables:

        #获取原始的hive表信息
        stdout = get_hive_table(DESC_FORMAT % (DATABASE, tablename))

        #处理获取到的hive表信息
        infos = check_table_info(tablename, stdout)

        # 要插入到Mysql的数据
        inst = infos["inst"]
        if inst.__len__() > 0:
            insert_infos(INSERT_FORMAT, inst)

        # 要更新到Mysql的灵气
        upda = infos["upda"]
        if upda.__len__() > 0:
            update_infos(UPDATE_FORMAT, upda)

# main function
if __name__ == '__main__':
    main()



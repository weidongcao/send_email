#!/usr/bin/python
# coding=utf-8

import re
from database_utils import *
from constant_util import *
from hive_util import *
# 根据文件获取文件内存放的表名
def get_tables(sqlfile):
    tables = []

    content = open(sqlfile)
    lines = content.readlines()
    for table in lines:
        tables.append(table)

    return tables

"""
对hive查询的表信息进行处理获取字段名，字段类型，字段注释
样本:
col_name        data_type       comment
mobile_no               string  desc
max_name                string
weigth_total            string
tag_psb                 array<string>
sum_p                   float
ds                      string

# Partition Information
# col_name              data_type               comment

ds                      string
处理hive返回的数据,将之转换成一个字典返回
"""
def get_table_desc(stdout):

    # 将原始数据字符串以行分割
    lines = stdout.split("\n")

    # 要返回的字典
    table_dict = {}

    # 标识是否为分区
    flat = False

    #以行为单位处理原始数据
    for index in range(len(lines)):

        line = lines[index]

        # 原始数据中有空行
        if line:
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
                if flat:
                    cols.append("True")
                else:
                    cols.append("False")

                # 以字段名为key,存储为字典
                table_dict[cols[0]] = cols
    return table_dict

# 处理hive表信息,看哪些信息是新增、修改、删除的，返回这些修改过的信息
def check_table_info(hive_tablename, stdout):

    # 处理hive原始输出信息
    hive_table_dict = get_table_desc(stdout)

    # 查询Mysql数据库相关记录
    # 创建需要字段需要替换的字典
    sql_temple_dict = [{"mysqltablename": TABLENAME, "tablename": hive_tablename}]

    # 常量模板替换
    sql_select_table = transform_format_string(SELECT_FORMAT, sql_temple_dict)
    # mysql数据库查询
    table_infos = select_multi_data(sql_select_table[0])

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
    if hive_table_dict > 0:
        for dcol in hive_table_dict:

            # 从hive获取到的一条字段信息
            dval = list(hive_table_dict[dcol])

            # Mysql中是否已经存储了这个表的信息，如果没有的话数据插入
            if mdict > 0:
                # Mysql中是否已经存储了这个表的这个字段,如果没有数据插入,如果如果存在的话比较字段其他信息是否被修改
                if mdict.has_key(dcol):

                    # Mysql中存储的这个表这个字段的数据
                    mval = list(mdict[dcol])

                    if ((mval.count(dval[1]) == 0)      # 判断字段类型是否修改
                        or (mval.count(dval[2]) == 0)     # 判断字段注释是否修改
                        or (mval.count(dval[3]) == 0)):          # 字段是否为分区字段是否修改

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
                if hive_table_dict.has_key(mcol):
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

def get_desc_table():
    # 需要监控的表名放在此文件里
    # file = "/home/caoweidong/python/formal/tables"
    table_file = "resource/tables"

    # 获取需要监控的表名列表
    tables = get_tables(table_file)

    # 分析每一个需要监控的表，将表信息存放在mysql数据库
    for tablename in tables:

        # 获取原始的hive表信息
        stdout = run_hive_sql(DESC_FORMAT % (DATABASE, tablename))
        # stdout = DEMO_DESC_TABLE

        # 处理获取到的hive表信息
        infos = check_table_info(tablename, stdout)

        # 要插入到Mysql的数据
        inst = infos["inst"]
        inst_list = []
        for datalist in inst:
            inst_dict = {}
            inst_dict["tablename"] = datalist[0]
            inst_dict["colname"] = datalist[1]
            inst_dict["coltype"] = datalist[2]
            inst_dict["tcomment"] = datalist[3]
            inst_dict["is_partition_field"] = datalist[4]
            inst_dict["colstatus"] = datalist[5]
            inst_list.append(inst_dict)

        sql_inst_list = transform_format_string(INSERT_FORMAT, inst_list)
        if sql_inst_list:
            batch_modify_database(sql_inst_list)
            logger.info("insert success")
        else:
            logger.info("no data to be insert")
        # if inst.__len__() > 0:
        #     insert_infos(INSERT_FORMAT, inst)

        # 要更新到Mysql的灵气
        upda = infos["upda"]
        upda_list = []
        for datalist in upda:
            upda_dict = {}
            upda_dict["tid"] = datalist[0]
            upda_dict["tablename"] = datalist[1]
            upda_dict["colname"] = datalist[2]
            upda_dict["coltype"] = datalist[3]
            upda_dict["tcomment"] = datalist[4]
            upda_dict["is_partition_field"] = datalist[5]
            upda_dict["colstatus"] = datalist[6]
            upda_list.append(upda_dict)
        sql_upda_list = transform_format_string(UPDATE_FORMAT, upda_list)
        if sql_upda_list:
            batch_modify_database(sql_upda_list)
            logger.info("update success")
        else:
            logger.info("no data to be update")

def main():
    get_desc_table()

# main function
if __name__ == '__main__':
    main()



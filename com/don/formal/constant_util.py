#!/usr/bin/python
# coding=utf-8

"""文件用于存储常量"""

# 表名
TABLENAME = "table_dictionary"

# 格式化的表描述语句
DESC_FORMAT = "desc %s.%s"

# 使用的哪个数据库
DATABASE = "leesdata"

# 格式化的table_dictionary表插入语句(9个占位符)
INSERT_FORMAT = "insert into %s(tid, tablename, colname, coltype, tcomment, is_partition_field, colstatus, create_date, update_date) values (null, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"

# 用于测试
DEMO_INSERT_SQL = INSERT_FORMAT % (TABLENAME, "idl_limao_address_raw_log", "ds", "string", "partition", "true", "use" , "2016-12-26", "2016-12-30")

# 格式化的更新sql(9个占位符)
UPDATE_FORMAT = "update %s set tablename = '%s', colname = '%s', coltype = '%s', tcomment = '%s', is_partition_field = '%s', colstatus = '%s', update_date = '%s' where tid = '%s'"

# 格式化的查询语句(2个点位符)
SELECT_FORMAT = "select tid, tablename, colname, coltype, tcomment, is_partition_field, colstatus, create_date, update_date from %s where tablename = '%s'"

# hive 表数据样本
DEMO_DESC_TABLE = """
col_name      data_type       comment
mobile_raw              string
moblie_no               string
moblie_type             string
mobile_province         string
mobile_city             string
mobile_operators        string
receiver_name           string
tid_num                 int
ds                      string

# Partition Information
# col_name              data_type               comment

ds                      string
"""
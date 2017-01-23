#!/usr/bin/python
# coding=utf-8

# 格式化的表描述语句
DESC_FORMAT = 'desc %s.%s'

# 格式化的table_dictionary表插入语句(9个占位符)
INSERT_FORMAT = 'insert into table_dictionary(tid, tablename, colname, coltype, tcomment, is_partition_field, colstatus, create_date, update_date) values (null, "{tablename}", "{colname}", "{coltype}", "{tcomment}", "{is_partition_field}", "{colstatus}", now(), null)'

TABLE_INST_DICT = {
    "tablename": "idl_limao_address_raw_log",
    "colname": "ds",
    "coltype": "string",
    "tcomment": "partition",
    "is_partition_field": "true",
    "colstatus": "use"
}
TABLE_UPDA_DICT = {
    "tablename": "idl_limao_address_raw_log",
    "colname": "ds",
    "coltype": "string",
    "tcomment": "partition",
    "is_partition_field": "true",
    "tid": "7",
    "colstatus": "use"
}
# 格式化的更新sql(10个占位符)
UPDATE_FORMAT = 'update table_dictionary set tablename = "{tablename}", colname = "{colname}", coltype = "{coltype}", tcomment = "{tcomment}", is_partition_field = "{is_partition_field}", colstatus = "{colstatus}", update_date = now() where tid = {tid}'

# 格式化的查询语句(2个点位符)
SELECT_FORMAT = 'select tid, tablename, colname, coltype, tcomment, is_partition_field, colstatus, create_date, update_date from table_dictionary where tablename = "{tablename}"'

HIVE_SELECT_COUNT_FORMAT = """use leesdata; select count(1) from {table_name} where ds = "{param}" group by ds;"""

MONITOR_DATA_SELECT_FORMAT = """SELECT job_id, job_name, param, table_name, table_type, check_status FROM result_target_log where check_status = 0 limit 2;"""

MONITOR_DATA_UPDATE_FORMAT = """update result_target_log set result = {result}, updatedt = now(), check_status = {check_status} where job_id = "{job_id}" and table_name = "{table_name}";"""

# 使用的哪个数据库
DATABASE = 'leesdata'

# 表名
TABLENAME = 'table_dictionary'

DEMO_DESC_TABLE = """col_name        data_type       comment
mobile_no               string
max_name                string
weigth_total            string
tag_psb                 array<string>
sum_p                   float
ds                      string

# Partition Information
# col_name              data_type               comment

ds                      string
"""
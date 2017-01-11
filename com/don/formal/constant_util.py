#!/usr/bin/python
# coding=utf-8

"""文件用于存储常量"""
# 主机名
MYSQL_HOSTNAME = "54.223.85.243"
# 用户名
MYSQL_USERNAME = "dc"
# 密码
MYSQL_PASSWORD = "mCdlUmm3thna5ttup"
# 要连接的数据库实例
# database = "datacenter"
MYSQL_DATABASE = "control_job_dev"
# 端口号
MYSQL_PORT = 3306

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
DEMO_DESC_TABLE = """col_name      data_type       comment
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

EMAIL_SELECT_ERROR_DATA_FOTMAT = """SELECT
    job.parameter as parameter,
    job.job_name as job_name,
    concat(
        '{',
        concat_ws(',',
            CONCAT_WS(':','\\'job_name\\'', CONCAT('\\'',job.job_name,'\\'')),
            CONCAT_WS(':','\\'data_desc\\'', CONCAT('\\'',job.data_desc,'\\'')),
            CONCAT_WS(':','\\'parameter\\'', CONCAT('\\'',job.parameter,'\\'')),
            CONCAT_WS(':','\\'task_total\\'', CONCAT('\\'',job.task_total,'\\'')),
            CONCAT_WS(':','\\'finsh_num\\'', CONCAT('\\'',job.finsh_num,'\\'')),
            CONCAT_WS(':','\\'status\\'', CONCAT('\\'',job.status,'\\''))
        ),
        '}'
    )
FROM
    running_job_log job
WHERE
    job.status < 0
    AND job.job_name = 'daily_run_job'
    AND job.job_id = '%s'
"""
EMAIL_SELECT_CONf_FOTMAT = """select
    cid, conf_name, temple_path, sender_email, sender_name,
        receiver_email, temple_path
from
    config_email_log
where
    conf_name = '{conf_name}'
"""

EMAIL_INSERT_DATA_FORMAT = """INSERT INTO running_email_log(
    rid, job_id,
    cid, sender_email,
    sender_name, email_theme,
    email_content, receiver_email,
    create_date)
VALUES (
    null, "{job_id}",
    {cid}, "{sender_email}",
    "{sender_name}", "{email_theme}",
    "{email_content}", "{receiver_email}",
    "{create_date}")
"""

EMAIL_SELECT_ERROR_JOB = """SELECT
    r.job_name,
    r.data_desc,
    r.parameter,
    r.task_total,
    r.finsh_num,
    r.status
FROM
    running_job_log r
WHERE
r.status < 0 and parameter = '2016-12-16'
AND r.job_name = 'daily_run_job'
"""
EMAIL_SELECT_RUNNING_LOG = """SELECT
    email.rid as rid,
    email.job_id as jobid,
    email.cid as cid,
    email.sender_email as sender_email,
    email.sender_name as sender_name,
    email.email_theme as email_theme,
    email.email_content as email_content,
    email.receiver_email as receiver_email,
    email.create_date as create_date
FROM
    running_email_log email
WHERE
email.job_id = '{job_id}'
"""
EMAIL_INSERT_LOGGING_LOG = """INSERT INTO logging_email_log(
    lid, cid, rid,
    job_id, send_date
    )
VALUES (
    null, {cid},{rid},
    '{job_id}', '{send_date}'
)
"""


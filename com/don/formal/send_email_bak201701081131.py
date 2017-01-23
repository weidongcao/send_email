#!/usr/bin/python
# -*- coding: utf8 -*-

from database_utils import *
from email_util import email_service
from constant_util import *

def create_email_error_data(job_id, conf_name):
    select_job_error_sql = EMAIL_SELECT_ERROR_DATA_FOTMAT % job_id
    error_info = select_single_data(select_job_error_sql)

    select_email_conf = EMAIL_SELECT_CONf_FOTMAT.replace('{conf_name}', conf_name)
    conf_info = select_single_data(select_email_conf)
    """select
        cid, conf_name, temple_path, sender_email, sender_name,
            receiver_email, temple_path
    from
        config_email_log
    where
        conf_name = '{conf_name}'
    """
    email_info = {}
    email_info['job_id'] = job_id
    email_info['cid'] = str(conf_info[0])
    email_info['sender_email'] = conf_info[3]
    email_info['sender_name'] = conf_info[4]
    email_info['email_theme'] = 'job : ' + error_info[1] + ' error in date : ' + error_info[0]
    email_info['email_content'] = error_info[2]
    email_info['receiver_email'] = conf_info[5]
    email_info['temple_path'] = conf_info[6]
    email_info['create_date'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

    return email_info

def send_email(job_id, conf_name):
    email_info = create_email_error_data(job_id, conf_name)
    sql_list = transform_temple_content(EMAIL_INSERT_DATA_FORMAT, [email_info])

    for counter in range(3):
        flat = insert_data(sql_list)
        if flat:
            break

    send_flat = email_service(email_info)

    # send_flat = True

    temp = {'job_id': email_info['job_id']}
    sql_select = transform_temple_content(EMAIL_SELECT_RUNNING_LOG, [temp])[0]

    row = select_single_data(sql_select)
    email_info['rid'] = str(row[0])
    email_info['send_date'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

    if send_flat:
        sql_list = transform_temple_content(EMAIL_INSERT_LOGGING_LOG, [email_info])
        insert_data(sql_list)

def main():
    job_id = "T3543198121286182"
    conf_name = "error_report"
    send_email(job_id, "error_report")


if __name__ == '__main__':
    main()
    # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
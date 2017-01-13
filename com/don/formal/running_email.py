#!/usr/bin/python
# -*- coding: utf8 -*-
import sys
import threading
import time
import re


from constant_sql_util import *
from email_util import *
from json_util import *
from logger import logger

reload(sys)
sys.setdefaultencoding("utf-8")

def get_running_list(select_sql):
    # 检查是否有未发送的邮件
    select_list = list(select_multi_data(select_sql))

    running_list = []
    for info_list in select_list:
        info_dict = {}

        # 任务ID
        info_dict["run_id"] = info_list[0]
        # 配置ID
        info_dict["conf_id"] = info_list[1]
        # 邮件ID
        info_dict["email_id"] = info_list[2]
        # 附件
        info_dict["attachment_name"] = info_list[3]
        # 模板
        info_dict["temple_path"] = info_list[4]
        # 图片
        info_dict["picture_name"] = info_list[5]
        # 邮件主题
        info_dict["email_theme"] = info_list[6]
        # 邮件内容
        info_dict["email_content"] = info_list[7]
        # 邮件内容的类型
        info_dict["email_content_type"] = info_list[8]
        # 邮件内容编码
        info_dict["email_content_encode"] = info_list[9]
        # 邮件展示的字段
        info_dict["email_content_col"] = info_list[10]
        # 发件人
        info_dict["sender_email"] = info_list[11]
        # 发件人名称
        info_dict["sender_name"] = info_list[12]
        # 发件人密码
        info_dict["sender_passwd"] = info_list[13]
        # 发件邮箱服务器
        info_dict["email_server"] = info_list[14]
        # 发件邮箱服务器端口
        info_dict["server_port"] = info_list[15]
        # 收件人列表
        info_dict["receiver_list"] = info_list[16]
        # 任务状态
        info_dict["run_status"] = info_list[17]
        # 生成时间
        info_dict["create_date"] = info_list[18]
        # 更新时间
        info_dict["update_date"] = info_list[19]

        # 将邮件内容的字段名和值相对应
        info_dict["email_content_convert"] = transform_kv_content(info_dict["email_content_col"], info_dict["email_content"])
        running_list.append(info_dict)
    return running_list

def transform_email_data(list_data):
    if list_data:
        try:
            # 对邮件内容字符串转换成json
            list_data = json.dumps(list_data)
        except Exception as e:
            logger.error(e.message)
            logger.error("string convert to json fail")
            return False
        return list_data
    else:
        logger.debug("fail_task in list_data is blank")
        return False

# 发送邮件
def send_email():

    # 调取存储过程检查并发送未发送及发送失败的邮件
    call_proc("proc_main")

    # 获取需要发送的邮件
    select_list = list(get_running_list(EMAIL_SELECT_RUNNING))

    if select_list:
        for msg_dict in select_list:
            # 模板路径
            temple_path = msg_dict["temple_path"]
            # 邮件内容
            # list_data = msg_dict["email_content_convert"]

            # 获取字段值
            list_data = transform_value_content(msg_dict["email_content"])

            # 将字段名字符串切分成列表
            list_key = msg_dict["email_content_col"].split(",")

            # break
            # 如果邮件内容字符串格式不正确则不发送邮件
            if list_data:
                # 替换模板生成要发送邮件的内容
                # html_content = transform_temple(temple_path, list_data, msg_dict["email_theme"])
                html_content = transform_temple2(temple_path, list_key, list_data, msg_dict["email_theme"])

                # 将邮件内容写入字典
                msg_dict["html_content"] = html_content

                # 发送邮件
                send_result = email_service(msg_dict)
                # send_result = True
                # 发送邮件的结果
                if send_result:
                    msg_dict["run_status"] = 1

                else:
                    msg_dict["run_status"] = -1

                # 生成任务表更新sql语句
                email_update_running_sql = transform_format_string(EMAIL_UPDATE_RUNNING, [msg_dict])
                #将发邮件的结果更新到数据库
                batch_modify_database(email_update_running_sql)
            else:
                logger.error("eamil content string is not formatted style")
    else:
        logger.info("no email to be send")
    time.sleep(15)

def run_app():
    flat = True
    while flat:
        try:
            send_email()
        except Exception as e:
            logger.error(e.message)
            flat = False

def main():
    logger.info("system start")
    flat = True
    while flat:
        send_email()


if __name__ == "__main__":
    main()

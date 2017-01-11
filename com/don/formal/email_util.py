#!/usr/bin/python
# -*- coding: utf8 -*-

#加载smtplib模块
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.header import Header
from database_utils import *
from logger import logger

# 邮件模板转换成要发送的邮件内容
def transform_temple(temple_path, data):
    content = open(temple_path)
    lines = content.readlines()
    htmlcontent = ''
    for line in lines:
        htmlcontent += line
    content.close()

    # 切分模板内容，取出需要替换的部分
    parts = htmlcontent.split('<!-- replace data -->')
    data_item = parts[1]

    # 替换模板内容
    data_item = transform_temple_content(data_item, data)

    htmlcontent = htmlcontent.replace(parts[1], "\n".join(data_item))
    logger.error("email content : \n" + htmlcontent)

    return htmlcontent

# 发送邮件
def email_service(msg_dict):

    # 收件人列表
    receiver_list = str(msg_dict['receiver_list']).split(",")

    try:
        msg = MIMEText(msg_dict['html_content'], msg_dict['email_content_type'], msg_dict['email_content_encode'])
        #括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['From'] = formataddr([msg_dict['sender_name'], msg_dict['sender_email']])

        #括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['To'] = ",".join(receiver_list)

        #邮件的主题，也可以说是标题
        msg['Subject'] = msg_dict['email_theme']
        #发件人邮箱中的SMTP服务器，端口是25
        server = smtpObj = smtplib.SMTP_SSL(msg_dict['email_server'], port=msg_dict['server_port'])
        #括号中对应的是发件人邮箱账号、邮箱密码
        server.login(msg_dict['sender_email'], msg_dict['sender_passwd'])
        #括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        # server.sendmail(msg_dict['sender_email'], msg_dict['receiver_list'], msg.as_string())
        server.sendmail(msg_dict['sender_email'], receiver_list, msg.as_string())
        #这句是关闭连接的意思
        server.quit()
        logger.info("send email success")
        ret = True
    #如果try中的语句没有执行，则会执行下面的ret=False
    except Exception as e:
        logger.error(e.message)
        logger.error("Error: send email fail")
        ret = False
    return ret


def main():
    massage = '上海爱快抵网络科技有限公司大数据任务调度发送的邮件'
    ret = email_service(massage)
    if ret:
        # 如果发送成功则会返回ok，稍等20秒左右就可以收到邮件
        print("ok")
    else:
        # 如果发送失败则会返回filed
        print("filed")

if __name__ == '__main__':
    # main()
    sss = "idl_limao_mobile_relation_raw_agg&idl_address_main_dim&idl_limao_supple_info_agg&idl_limao_tid_relation_log&idl_limao_cid_tmp&idl_limao_cid_price_agg&idl_limao_receiver_relation_raw_agg&idl_limao_address_raw_log"
    sss = sss.replace(",", " <br />")
    print("sss = " + sss)

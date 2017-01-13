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
def transform_temple(temple_path, data, theme):
    content = open(temple_path)
    lines = content.readlines()
    htmlcontent = ''
    for line in lines:
        htmlcontent += line
    content.close()

    htmlcontent = htmlcontent.replace("{email_theme}", theme)

    # 切分模板内容，取出需要替换的部分
    parts = htmlcontent.split('<!-- replace data -->')
    data_item = parts[1]

    # 替换模板内容
    data_item = transform_temple_content(data_item, data)

    htmlcontent = htmlcontent.replace(parts[1], "\n".join(data_item))
    logger.error("email content : \n" + htmlcontent)

    return htmlcontent

"""
邮件模板转换成要发送的邮件内容
这个方法及调用的模板更加通用,方法会根据输入的key_list和val_list的长度进行动态修改
参数说明:
temple_path     模板的路径
key_list        要显示的表头,是一个list数组形式如下：[colname1, colname2, colname3]
val_list        要显示的数据,是一个双重list数组,形式如下:[[colvalue1, colvalue2, colvalue3], [colvalue4, colvalue5, colvalue6]]
key_list和val_list内部数组的长度不一致的话以最长的为准,不够长的key_list的值以unknown colunm显示, val_list的值以error input显示
"""
def transform_temple2(temple_path, key_list, val_list, theme):
    content = open(temple_path)
    lines = content.readlines()
    htmlcontent = ''
    for line in lines:
        htmlcontent += line
    content.close()


    # 校验key_list 和val_list的长度,如果不一样长的话以最长的为准
    max_len = key_list.__len__()

    # 校验val_list内部每一个list的长度,以最长的为准
    for item in val_list:
        if item.__len__() > max_len:
            max_len = item.__len__()

    # 如果key_list的长度小于最大长度,对key_list添加
    if max_len > key_list.__len__():
        add_len = max_len - key_list.__len__()
        for index in range(add_len):
            key_list.append("unknown column")

    # 如果val_list的长度小于最大长度,对val_list内每一个list进行添加
    transform_val_list = []
    for item_list in val_list:
        if max_len > item_list.__len__():
            add_len = max_len - item_list.__len__()
            for index in range(add_len):
                item_list.append("error input")
        transform_val_list.append(item_list)

    # 替换邮件主题
    htmlcontent = htmlcontent.replace("{email_theme}", theme)

    # 切分模板内容，取出需要替换的部分
    parts = htmlcontent.split('<!-- replace data -->')

    #表格头部
    table_title_temple = parts[1]
    # 表格内容
    table_content_temple = parts[3]

    # 替换模板表格头部
    table_title = create_email_data(table_title_temple, key_list)

    # 替换模板表格内容
    table_content = create_email_item(table_content_temple, transform_val_list)

    htmlcontent = htmlcontent.replace(parts[1], "\n".join(table_title))
    htmlcontent = htmlcontent.replace(parts[3], "\n".join(table_content))

    return htmlcontent

"""
就是要把下面的模板
<tr>
    <!-- replace td -->
    <td >{info_data}</td>
    <!-- replace td -->
</tr>
替换成：
<tr>
    <td>data1</td>
    <td>data1</td>
    <td>data1</td>
</tr>
<tr>
    <td>data1</td>
    <td>data1</td>
    <td>data1</td>
</tr>
"""
def create_email_item(temple, data_list):

    # 对模板再次进行分割
    inner_parts = temple.split('<!-- replace td -->')

    table_content = []
    for item in data_list:
        # 替换模板
        if item:
            table_tds = create_email_data(inner_parts[1], item)
            table_tr = temple.replace(inner_parts[1], "\n".join(table_tds))
            table_content.append(table_tr)

    return table_content


"""
替换模板:将｛key｝替换为value
模板如下：
<td >{info_data}</td>
data_list 是一个数组
"""
def create_email_data(temple, data_list):
    # 判断模板是否为空
    if temple:
        # 判断要替换的数据是否为空
        if data_list:
            info_list = []
            data_list = list(data_list)
            # 替换模板
            for data in data_list:
                info = temple
                try:
                    info = info.replace('{info_data}', str(data))
                    info_list.append(info)
                except Exception as e:
                    logger.error(e.message)
                    logger.error("convert error : value = " + str(data))
                    info_list = False
                    break
        else:
            logger.error("data_list is none")
            info_list = False
    else:
        logger.error("temple is none")
        info_list = False
    return info_list



def transform_kv_content(key_string, value_string):
    if key_string and value_string:
        value_string = value_string.replace(":", "-")
        content_list = []
        key_list = key_string.split(",")
        value_list = value_string.split("__$__")
        for item in value_list:
            if item:
                dict = {}
                info_list = item.split("__&__")
                for index in range(info_list.__len__()):
                    if index <= key_list.__len__() - 1:
                        key = key_list[index].strip()
                        dict[key] = info_list[index]
                    else:
                        logger.error("value number is more than key number")
                content_list.append(dict)

    else:
        logger.error("content key or contvalue value is blank or not formatted")
        content_list = False
    return content_list

def transform_value_content(value_string):
    if value_string:
        value_string = value_string.replace(":", "-")
        content_list = []
        value_list = value_string.split("__$__")
        for item in value_list:
            if item:
                info_list = item.split("__&__")
                content_list.append(info_list)

    else:
        logger.error("content key or contvalue value is blank or not formatted")
        content_list = False
    return content_list
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

    aaa = ['a', 'b', 'c']
    for index in range(aaa.__len__()):
        print(aaa[index])

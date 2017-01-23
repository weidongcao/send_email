#!/usr/bin/python
# -*- coding: utf8 -*-

import smtplib
from email.mime.text import MIMEText
from email.header import Header

# 第三方 SMTP 服务
#设置服务器
mail_host = "smtp.exmail.qq.com"
#用户名
mail_user = "caoweidong@ikuaidi.com"
#口令
mail_pass = "888888aA"


sender = 'caoweidong@ikuaidi.com'
receivers = '1774104802@qq.com'  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

message = MIMEText('caoweidong', 'plain', 'utf-8')
message['From'] = 'dong<caoweidong@ikuaidi.com>'
message['To'] = '1774104802@qq.com'
# message['From'] = Header("caoweidong", 'utf-8')
# message['To'] = Header("caoweidong", 'utf-8')

subject = 'caoweidong'
message['Subject'] = Header(subject, 'utf-8')


try:
    smtpObj = smtplib.SMTP_SSL("smtp.exmail.qq.com", port=465)
    smtpObj.login(mail_user, mail_pass)
    smtpObj.sendmail(sender, receivers, message.as_string())
    smtpObj.close()
    print "邮件发送成功"
except smtplib.SMTPException as e:
    print "Error: 无法发送邮件"
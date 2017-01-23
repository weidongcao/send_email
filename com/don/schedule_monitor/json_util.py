#!/usr/bin/python
#coding=utf-8
import json
import os

from logger import logger

def get_json(json_file):
    data_file = json_file
    if os.path.isfile(data_file):
        with open(data_file, 'r') as f:
            data = f.read()
            if data:
                return json.loads(data)
            else:
                return False
    else:
        open(data_file, 'w')
        print '载入历史数据'

def save_json(json_file, queue_data):
    data_file = json_file
    with open(data_file, 'w') as f:
        f.write(json.dumps(queue_data))
    print '保存数据'

def convert_to_json(str_list):
    try:
        if (len(str_list) != 0) and (str_list != ''):
            json_list = []
            data_list = str_list.split(";")
            for item in data_list:
                if (len(item) != 0) and (item != ''):
                    info_list = item.split("|")
                    dict_info = {}
                    for info in info_list:
                        if info:
                            keyvalue = info.split(":")
                            if(keyvalue.__len__() == 2):
                                dict_info[keyvalue[0]] = keyvalue[1]
                            else:
                                logger.error("key value convert error the data is : " + keyvalue)
                                return False
                    json_list.append(dict_info)
    except Exception as e:
        logger.error(e.message)
        logger.error(("转变成Json失败"))
        return False
    json_data = json.dumps(json_list)
    logger.debug("convert_to_json : \n" + json_data)
    return json_data

if __name__ == '__main__':
    # poor = get_json("D:\\bigdata\\workspace\\PycharmProjects\\python\\com\\don\\formal\\resource\\application_content.json")
    # print(poor["mainbase"])
    str = "job_name:job_daily|job_id:S10542451693250288|data_desc:taskname1, taskname2, taskname3,taskname4, taskname5, taskname6,taskname7, taskname8, taskname9|parameter:2017-01-10|insertdt:2017-01-10 20-20-19; job_name:job_daily|job_id:T32977120143340084|data_desc:taskname1, taskname2, taskname3,taskname4, taskname5, taskname6|parameter:2017-01-09|insertdt:2017-01-09 22-22-19; job_name:job_daily|job_id:T32977120143340084|data_desc:taskname1, taskname2, taskname3|parameter:2017-01-08|insertdt:2017-01-09 22-22-19;"
    json_str = convert_to_json(str)
    print(json_str)

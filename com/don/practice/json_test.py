#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import os

def load_data(json_file):
    data_file = json_file
    if os.path.isfile(data_file):
        with open(data_file, 'r') as f:
            data = f.read()
            if data:
                return json.loads(unicode(data))
            else:
                return False
    else:
        open(data_file, 'w')
        print '载入历史数据'

def save_data(json_file, queue_data):
    data_file = json_file
    with open(data_file, 'w') as f:
        f.write(json.dumps(queue_data))
    print '保存数据'

if __name__ == "__main__":
    poor = load_data("D:\\bigdata\\workspace\\PycharmProjects\\python\\com\\don\\formal\\resource\\application_content.json")

    print("poor['mainbase']['database'] = " + poor['mainbase']['database'])

    json_database = json.dumps(poor['mainbase'])

    json_dic2 = json.dumps(poor['mainbase'], sort_keys=True, indent=4, separators=(',', ':'), encoding="utf-8",ensure_ascii=True)
    print(json_database)
    print(json_dic2)

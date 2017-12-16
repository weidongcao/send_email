#!/usr/bin/python
# coding=utf-8
import calendar
import datetime
import json
import os
import subprocess
import sys
import urllib2

reload(sys)
sys.setdefaultencoding('utf-8')

# 脚本使用说明
# 初次使用需要根据集群情况修改以下参数
# host_name: 可以是Solr集群中的任意一台服务器的主机名或者IP地址
# solr_port: 此服务器Solr的端口号
# numShards: Solr集群节点数
# replicationFactor: 副本数，数据量不大可以指定副本数为1，数据量大的话指定为2

# 创建Collection的规则
# 脚本的功能为Solr每个月10天创建一个Collection
# Collection的命令格式为"yisou年月(天除了10)"
# 例如
# 2017年12月03日在2017年12月的第1个10天，此Collection命名为yisou20171201
# 2017年12月13日在2017年12月的第2个10天，此Collection命名为yisou20171202
# 2017年12月23日在2017年12月的第3个10天，此Collection命名为yisou20171203
# 2017年12月30日31日，就两天，也把它算到第3个10天，命令为yisou20171203

# 执行定时任务
# 定时执行此任务每天的凌晨3点执行一次
# 服务器重启执行一次
# crontab 定时执行命令：
# * 3 * * * python create_solr_collection.py
# @reboot python create_solr_collection.py

# 全局变量
# Solr所在的服务器主机名或IP
host_name = "http://cm02.spark.com"
# Solr的端口号
solr_port = 8080
# 分片数
numShards = 3
# 副本数
replicationFactor = 1
# 一个节点最多多少个分片
maxShardsPerNode = 1
# Collection归属
project_identify = "yisou"

# 获取Solr所有Collection的HTTP请求模板
get_all_collection_url_template = """ ${host_name}:${solr_port}/solr/admin/collections?action=LIST&wt=json """
# 生成Solr Collection的HTTP请求模板
create_collection_url_template = """${host_name}:${solr_port}/solr/admin/collections?action=CREATE&name=${\
collection_name}&numShards=${numShards}&replicationFactor=${replicationFactor}&maxShardsPerNode=${\
maxShardsPerNode}&collection.configName=${collection_conf}&wt=json """

# 为所有Collection创建别名的模板
create_alias_url_template = """${host_name}:${solr_port}/solr/admin/collections?action=CREATEALIAS&name=collection\
&collections=${all_collection}&wt=json """

# 测试，创建Collection成功的返回结果
data_create_collection_success = """
{"responseHeader":{"status":0,"QTime":6400},"success":{"cm02.spark.com:8080_solr":{"responseHeader":{"status":0,"QTime":1722},"core":"yisou20171101_shard2_replica1"},"cm03.spark.com:8080_solr":{"responseHeader":{"status":0,"QTime":1734},"core":"yisou20171101_shard3_replica1"},"cm01.spark.com:8080_solr":{"responseHeader":{"status":0,"QTime":1725},"core":"yisou20171101_shard1_replica1"}}}
"""

delete_collection_url_template = """
${host_name}:${solr_port}/solr/admin/collections?action=DELETE&name=${collection}&wt=json
"""


# 执行执行Linux命令
def exec_cmd(full_command, cwd_path):
    """
    函数功能说明：Python执行Shell命令并返回命令执行结果的状态、输出、错误信息、pid

    第一个参数(full_command)：完整的shell命令
    第二个参数(pwd_path)：执行此命令所在的根目录

    返回结果：
        stdout:执行Shell命令的输出
        stderr:执行Shell命令的错误信息
        return_code:执行Shell命令结果的状态码
        pid:执行Shell程序的pid
    """
    try:
        process = subprocess.Popen(full_command, cwd=cwd_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, shell=True)

        while True:
            format_print(process.stdout.readline(), )
            if process.poll() is not None:
                break
        print("")
        # 等命令行运行完
        process.wait()

        # 获取命令行输出
        stdout = process.stdout.read()

        # 获取命令行异常
        stderr = process.stderr.read()
        # print("stderr = " + str(stderr))

        # 获取shell 命令返回值,如果正常执行会返回0, 执行异常返回其他值
        return_code = process.returncode
        # print("return_code = " + str(return_code))

        # 获取命令运行进程号
        pid = process.pid

        result_dict = {"stdout": stdout, "stderr": stderr, "return_code": return_code, "pid": pid}
        return result_dict
    except Exception as e:
        print(e.message)
        print("程序执行失败,程序即将退出")
        os._exit(0)
        return False


# 发起HTTP请求
def exec_http_request(url):
    req = urllib2.Request(url)
    response = urllib2.urlopen(req).read()
    ss = response.encode('utf-8')

    return json.loads(ss)


# 格式化打印
def format_print(content):
    prefix_info = "[" + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S") + " INFO] "

    print(prefix_info + content)


# 获取要在Linux命令行执行的curl命令
def get_solr_url_cmd(url):
    return "curl " + url


# 由当前日期根据指定规则生成Collection名称,如:yisou20171200,表示项目类型为yisou,
# 这个Collection里存储的是2017年12月1日到2017年12月10日的数据
def get_collection_name_by_date(key, cur_date):
    # 获取年份
    cur_year = cur_date.strftime("%Y")
    # 获取月份
    cur_month = cur_date.strftime("%m")
    # 获取日
    cur_day = cur_date.strftime("%d")

    # 如果日期大于等于30号不再单独创建Collection
    if int(cur_day) >= 30:
        cur_day = str(int(cur_day) - 5)

    # 10天创建一个一个Collection，如1-10号，11-20号，21-最后
    identify = (int(cur_day) - 1) / 10
    if len(str(identify)) == 1:
        identify = "0" + str(identify)

    # Collection的名称由项目标识+年+月+时间段标识
    collection_name = key + cur_year + cur_month + identify
    format_print("日期: " + cur_date.strftime("%Y-%m-%d") + " 此时间段的Collection: " + collection_name)

    return collection_name


# 根据当前Collection的名称获取到前一Collection的名称(yisou20171200)
def get_previous_collection_name(collection_name):
    # 获取日期标识
    identify = collection_name[-2::1]
    # 获取月份
    cur_month = collection_name[-4:-2:1]
    # 获取年份
    cur_year = collection_name[-8:-4:1]

    # 如果日标识为00则上一个一个Collection是上一个月的
    if str(identify) == "00":
        identify = "02"
        # 如果月份是01，则上一个一个Collection的月份标识是上一年的
        if str(cur_month) == "01":
            cur_year = str(int(cur_year) - 1)
            cur_month = "12"
        else:
            cur_month = str(int(cur_month) - 1)
    else:
        identify = str(int(identify) - 1)

    # 统一月份和日标识的长度
    if len(str(identify)) == 1:
        identify = "0" + str(identify)

    if len(cur_month.__str__()) == 1:
        cur_month = "0" + str(cur_month)

    # 生成前一个一个Collection的名称
    previous_collection_name = collection_name[:-8:1] + cur_year + cur_month + str(identify)

    format_print("生成的前一Collection名称： " + previous_collection_name)

    return previous_collection_name


# 根据当前日期获取Collection要保存的数据的时间段
def get_period_by_date(cur_date):
    # 获取到日期的日(一个月的第几天)
    cur_day = cur_date.day

    # 一个月的第一天
    first_date = cur_date + datetime.timedelta(days=(-cur_day + 1))

    # 按月10天创建一个一个Collection，如1号到10号创建一个一个Collection
    # 11号到20号创建一个一个Collection
    # 21号到最后创建一个一个Collection
    # 几号就从1号开始向后偏移对应的天数
    if 0 < cur_day <= 10:
        start_date = first_date
        end_date = first_date + datetime.timedelta(days=(10 - 1))
    elif 10 < cur_day <= 20:
        start_date = first_date + datetime.timedelta(days=10)
        end_date = first_date + datetime.timedelta(days=(20 - 1))
    else:
        start_date = first_date + datetime.timedelta(days=20)
        end_date = get_last_day_of_month(cur_date)

    # Collection的开始时间
    start_date_str = datetime.datetime.strftime(start_date, "%Y-%m-%d")

    # Collection的结束时间
    end_date_str = datetime.datetime.strftime(end_date, "%Y-%m-%d")

    return [start_date_str, end_date_str]


# 根据Collection名称获取它所存储的数据的时间段
def get_period_by_collection_name(collection_name):
    # 获取Collection名称的时间标识
    date_identify = collection_name[-8::1]

    # 获取日标识
    identify = int(collection_name[-1::1])

    # 获取该时间段内的任意一天
    date_identify = str(int(date_identify) + 5 + (identify * 10))
    # 字符串转为日期
    date_period = datetime.datetime.strptime(date_identify, "%Y%m%d")

    return get_period_by_date(date_period)


# 根据日期获取所在月的最后一天
def get_last_day_of_month(cur_date):
    cur_year = cur_date.year
    cur_month = cur_date.month
    cur_day = cur_date.day

    # 获取这个月的多少天
    days = calendar.monthrange(cur_year, cur_month)[1]

    # 在当前日期的基础上加上对应的天数
    return cur_date + datetime.timedelta(days=(days - cur_day))


# 根据Collection名称获取到下一个一个Collection的名称
def get_forward_collection_name(collection_name):
    # 获取日标识
    identify = collection_name[-2::1]
    # 获取月份
    cur_month = collection_name[-4:-2:1]
    # 获取年份
    cur_year = collection_name[-8:-4:1]

    # 如果日标识为02，表示这个Collection存储的是一个月最后10天的数据，
    # 下一个一个Collection就是下一个月的了
    if str(identify) == "02":
        # 重新开始日标识
        identify = "00"
        # 如果是12月，则下一月就过年了
        if str(cur_month) == "12":
            cur_year = str(int(cur_year) + 1)
            cur_month = "1"
        else:
            cur_month = str(int(cur_month) + 1)
    else:
        identify = str(int(identify) + 1)

    # 统一月份和日标识的长度为2，如果不够的话前面补0
    if len(str(identify)) == 1:
        identify = "0" + str(identify)

    if len(str(cur_month)) == 1:
        cur_month = "0" + str(cur_month)

    # 生成下一个一个Collection的名称
    forward_collection_name = collection_name[:-8:1] + cur_year + cur_month + identify
    format_print("Collection名称: " + collection_name + " 生成的下时间段的Collection名称: " + forward_collection_name)

    return forward_collection_name


# 获取Solr所有Collection
def get_all_collection():
    get_all_collection_url = get_all_collection_url_template.replace("${host_name}", host_name)
    get_all_collection_url = get_all_collection_url.replace("${solr_port}", solr_port.__str__())

    format_print("获取Solr所有的Collection ...")

    # 去SOlr集群查询
    response = exec_http_request(get_all_collection_url)
    collections = response["collections"]

    # 查询返回的结果是unicode编码，转为string
    col_utf8 = []
    for col in collections:
        col_utf8.append(col.encode("utf-8"))
    format_print("获取成功, Solr已创建的所有Collection: " + col_utf8.__str__())

    return col_utf8


# 创建前一个Collection
# 递归创建前一个一个Collection，如果前一个一个Collection没有创建的话，后面所有的Collection都不能创建
# 这样所有的Collection就都创建了
def create_previous_collection(collections, cur_collection_name):
    # 判断当前Collection是否已经创建
    if cur_collection_name not in collections:
        # 获取前一个Collection的名称
        prev_collection = get_previous_collection_name(cur_collection_name)

        # 递归创建前一个Collection，如果前一个Collection已经创建直接返回True
        create_previous_collection_result = create_previous_collection(collections, prev_collection)

        # 如果前一个一个Collection已经创建成功才创建下一个一个Collection
        if create_previous_collection_result:

            # 执行创建Collection
            create_cur_collection_result = create_collection(cur_collection_name)

            # 如果Collection创建成功把这个Collection添加到列表并返回
            if create_cur_collection_result:
                collections.append(cur_collection_name)
                return True
            else:
                return False
    else:
        return True


# 从指定的时间开始创建Collection
# 本来想用重载的，但是但是Python不支持重载
def create_previous_collection_until_end_date(collections, cur_collection_name, start_date):
    # 如果没有传开始创建的日期则默认从当前日期开始
    if start_date is None:
        start_date = datetime.datetime.now()

    # 获取当前日期的Collection的名称
    collection_name_temp = get_collection_name_by_date(project_identify, start_date)

    # 获取当前日期的Collection的前一个一个Collection
    start_collection_name = get_previous_collection_name(collection_name_temp)

    # 将前一个Collection添加到列表，则递归的时候到这个Collection就停了
    collections.append(start_collection_name)

    # 从下一个Collection递归创建Collection
    create_status = create_previous_collection(collections, cur_collection_name)

    # 把当前日期的Collection的前一个Collection从列表中移除，后面还要根据这些Collection列表为它们创建别名
    collections.remove(start_collection_name)

    return create_status


# 创建Collection
def create_collection(collection_name):
    # 替换创建模板
    create_collection_url = create_collection_url_template.replace("${host_name}", host_name)
    create_collection_url = create_collection_url.replace("${solr_port}", solr_port.__str__())
    create_collection_url = create_collection_url.replace("${numShards}", numShards.__str__())
    create_collection_url = create_collection_url.replace("${replicationFactor}", replicationFactor.__str__())
    create_collection_url = create_collection_url.replace("${maxShardsPerNode}", maxShardsPerNode.__str__())
    create_collection_url = create_collection_url.replace("${collection_name}", collection_name)
    create_collection_url = create_collection_url.replace("${collection_conf}", project_identify)

    format_print("创建Collection: " + collection_name + " ...")

    # 开始创建Collection
    response = exec_http_request(create_collection_url)
    # response = json.loads(data_create_collection_success)

    # 判断是否创建成功
    if "success" in str(response):
        # 获取此Collection要存储的数据的时间段
        start_end = get_period_by_collection_name(collection_name)

        format_print("已为 " + start_end[0] + " 到 " + start_end[1] + " 期间创建Collection: " + collection_name)
        format_print("")

        return True
    else:
        return False


# 删除Collection
def delete_collections(collections):
    # 删除请求模板替换主机名和端口号
    delete_collection_url_temp = delete_collection_url_template.replace("${host_name}", host_name)
    delete_collection_url_temp = delete_collection_url_temp.replace("${solr_port}", solr_port.__str__())

    # 如果Solr集群中根本没有没有Collection直接返回
    if collections.__len__() == 0:
        format_print("没有需要删除的Collection")
        return

    # 循环删除列表里的Collection
    for collection in collections:

        # 替换要删除的Collection
        delete_collection_url = delete_collection_url_temp.replace("${collection}", collection)

        # 执行删除
        format_print("即将删除Collection：" + collection.__str__())
        response = exec_http_request(delete_collection_url)

        # 判断是否删除成功，这个删除成功了才能删除下一个
        if "success" in str(response):
            format_print("Collection删除成功：" + collection.__str__())

        # format_print("response = " + str(response))


# 删除所有Collection
def delete_all_collections():
    collections = get_all_collection()
    delete_collections(collections)


# 为多个多个Collection创建别名
def create_alias(alias_name, collections):
    # 替换请求模板参数
    create_alias_url = create_alias_url_template.replace("${host_name}", host_name)
    create_alias_url = create_alias_url.replace("${solr_port}", solr_port.__str__())
    create_alias_url = create_alias_url.replace("${all_collection}", ",".join(collections))

    # 执行创建
    format_print("为Solr所有Collection创建别名:" + alias_name)
    response = exec_http_request(create_alias_url)
    # response = json.loads(data_create_collection_success)

    # 判断是否删除成功
    if int(response["responseHeader"]["status"]) == 0:
        format_print("为Solr所有Collection创建别名成功")
        return True
    else:
        format_print("为Solr所有Collection创建别名成功")
        return False


# 初始化参数
def init(hostname, port, shards, replicas):
    # 指定全局变量
    global host_name, solr_port, numShards, replicationFactor, maxShardsPerNode

    # 主机名
    host_name = hostname

    # 端口号
    solr_port = port

    # 分片数
    numShards = shards

    # 副本数
    replicationFactor = replicas

    # 单个节点最大分片数
    maxShardsPerNode = replicas


# 主程序
def main(args):
    # 如果没有传日期的参数就以系统当前的日期为准
    if args.__len__() > 1:
        cur_date = datetime.datetime.strptime(args[1], '%Y-%m-%d')
    else:
        cur_date = datetime.datetime.now()

    # 根据规则生成当前时间段的Collection的名称
    cur_collection_name = get_collection_name_by_date(project_identify, cur_date)
    # 根据当前时间段的Collection的名称取得下一个时间段的Collection名称
    forward_collection_name = get_forward_collection_name(cur_collection_name)

    format_print("当前时间段的Collection名称: " + cur_collection_name)
    format_print("下一时间段的Collection名称: " + forward_collection_name)

    # 获取Solr集群中所有的Collection
    collections = get_all_collection()

    # Solr中是否有Collection，如果有的话先判断判断Collection是否已经创建
    create_collection_result = False
    if collections.__len__() > 0:
        # 判断下一Collection是否已经创建，如果已经创建，直接返回，如果没有创建，先判断前面的Collection是否已经创建
        if forward_collection_name in collections:
            format_print("当时时间段及下一时间段的Collection都已经创建，不需要再创建")
        else:
            # 如果Solr里已经有Collection，判断前面的Collection是否已经创建, 并创建当前时间段和下一时间段的Collection
            create_collection_result = create_previous_collection(collections, forward_collection_name)
    else:
        # 如果Solr里一个一个Collection都没有，则创建当前时候段的Collection及下一时间段的Collection
        create_collection_result = create_previous_collection_until_end_date(collections, forward_collection_name, None)

    # 为所有Collection创建别名
    if create_collection_result:
        # 为所有的Collection创建别名
        create_alias(project_identify + "-all", collections)


""" 程序入口 """
if __name__ == "__main__":
    # 初始化参数
    # 第一个参数：Solr集群中任意一台服务器的主机名或IP地址
    # 第二个参数：该服务器中Solr服务对应的商品号
    # 第三个参数：Solr集群中的节点数
    # 第四个参数：Solr副本数（数据量小为1，数据量大为2）
    init("http://cm02.spark.com", 8080, 3, 1)
    # init("http://data2.hadoop.com", 8080, 2, 2)

    # 定时创建Collection和所有Collection的别名的主程序
    # main(["create_solr_collection.py", "2018-02-23"])

    main(sys.argv)
    # delete_all_collections()
    # create_collection(project_identify + "20170900")

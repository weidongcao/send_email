#!/usr/bin/python
# coding=utf-8

import subprocess

from database_utils import *

# 获取原始的表信息
def run_hive_sql(hivesql):
    try:
        logger.error("execute hive : \n" + hivesql)
        # 执行Linux命令,通过hive -e连接到hive并执行命令
        run_hive = subprocess.Popen(["hive", "-e", hivesql], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # 等待子线程执行完毕
        run_hive.wait()

        # 获取原始输出
        stdout, stderr = run_hive.communicate()

        # 这是测试用,开发完后注释掉
        # stdout = DEMO_DESC_TABLE
        returncode = run_hive.returncode
        result_dict = {"stdout": stdout, "stderr": stderr, "returncode": returncode}
        logger.info("executor hive sql success")
        return result_dict
    except Exception as e:
        logger.error(e.message)
        return False


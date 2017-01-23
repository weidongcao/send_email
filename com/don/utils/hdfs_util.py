#!/usr/bin/python
# -*- coding: utf8 -*-

import os
import time
import sys
import gzip
import subprocess
import re
reload(sys)
sys.setdefaultencoding("utf-8")

"""
将hdfs文件下载到本地并解压返回解压后的文件的文件名加路径
参数说明:
hdfs_file_url:hdfs文件的路径加文件名
save_dir:保存的路径
存储的文件名根据时间生成
"""


def get_unzip_hdfs_file(hdfs_file_url, save_dir):
    # 判断保存路径是否存在,不存在的话创建此目录
    if os.path.isdir(save_dir):
        pass
    else:
        os.mkdir(save_dir, 0777)

    # hdfs文件名
    filename = hdfs_file_url.split("/").pop()

    # 保存到本地的文件名
    save_filename = ""

    # 判断是否为压缩文件
    if filename.endswith(".gz"):
        save_filename = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time())) + ".gz"
    else:
        save_filename = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))

    # 判断保存路径最后是否有/
    if save_dir.endswith("/"):
        save_file = save_dir + save_filename
    else:
        save_file = save_dir + "/" + save_filename

    # 生成下载hdfs文件的命令
    hadoop_get = 'hadoop fs -get %s %s' % (hdfs_file_url, save_file)

    # shell执行生成的hdfs命令
    try:
        os.system(hadoop_get)
    except Exception as e:
        print(e)
        return False

    # 判断下载的hdfs文件是否为压缩文件
    if save_file.endswith(".gz"):

        # 对此压缩文件进行压缩
        try:
            # 解压后的文件名
            f_name = save_file.replace(".gz", "")
            # 解压缩
            g_file = gzip.GzipFile(save_file)
            # 写入文件
            open(f_name, "w+").write(g_file.read())
            # 关闭文件流
            g_file.close()

            return f_name
        except Exception as e:
            print(e)
            return False
    else:
        return save_file

def get_unzip_hdfs_file_from_dir(hdfs_dir, save_dir):
    #命令:获取hdfs目录下的文件
    hadoop_ls = "hadoop fs -ls %s | grep -i '^-'" % hdfs_dir

    # 执行shell命令
    hdfs_result = exec_sh(hadoop_ls, None)

    # 获取命令执行输出
    # hdfs_stdout = hdfs_result["stdout"]
    hdfs_stdout = """-rw-r--r--   2 caoweidong supergroup      42815 2017-01-23 14:20 /user/000000_0.gz
-rw-r--r--   2 caoweidong supergroup      42815 2017-01-23 17:01 /user/20170123162822.gz
-rw-r--r--   2 caoweidong supergroup      42815 2017-01-23 17:01 /user/201701231701.gz"""

    # 要下载的HDFS文件列表
    hdfs_list = []

    # 判断是否有输出
    if hdfs_stdout:

        # 以行分割, 一行是一个文件的信息
        hdfs_lines = hdfs_stdout.split("\n")

        # 对每一行进行处理
        for line in hdfs_lines:

            # 以空白字符为分割符获取hdfs文件名
            line_list = re.split("\s+", line)

            # -rw-r--r--   2 caoweidong supergroup      42815 2017-01-23 14:20 /user/000000_0.gz
            if line_list.__len__():
                # print("line_list[7] = " + line_list[7])

                # HDFS文件加入下载列表
                hdfs_list.append(line_list[7])
            else:
                pass
        # 下载文件
        for file in hdfs_list:
            get_unzip_hdfs_file(file, save_dir)
        return True
    else:
        return False


"""
执行shell命令
参数说明：
commandfull:命令
cwdpath:执行命令时所在的目录

返回值说明:
返回值是一个字典
stdout:命令的输出
stderr:命令的错误输出
returncode:命令执行返回值,0 -->执行成功 大于0执行失败, None-->还没有执行完
"""
def exec_sh(commandfull, cwdpath):
    try:
        result_cmd = subprocess.Popen(commandfull, cwd=cwdpath, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, shell=True)
        # 等等命令行运行完
        result_cmd.wait()

        # 获取命令行输出
        stdout = result_cmd.stdout.read()
        # print("stdout = " + str(stdout))

        # 获取命令行异常
        stderr = result_cmd.stderr.read()
        # print("stderr = " + str(stderr))

        # 获取shell 命令返回值,如果正常执行会返回0, 执行异常返回其他值
        returncode = result_cmd.returncode
        # print("returncode = " + str(returncode))

        # 获取命令运行进程号
        pid = result_cmd.pid

        result_dict = {"stdout": stdout, "stderr": stderr, "returncode": returncode, "pid": pid}
        return result_dict
    except Exception as e:
        print(e.message)
        return False

if __name__ == "__main__":
    # download_hdfs_file("/user/000000_0.gz", "/home/caoweidong/data/unzipDirectory")
    # result = get_unzip_hdfs_file("/user/000000_0.gz", "/home/caoweidong/data/")
    # print("get_unzip_hdfs_file = " + result)
    # un_gz("/home/caoweidong/data/000000_0.gz")
    # get_unzip_hdfs_file_from_dir(None, None)
    aaa = "-rw-r--r--   2 caoweidong supergroup      42815 2017-01-23 14:20 /user/000000_0.gz"
    alist = re.split("\s+", aaa)
    print(alist)
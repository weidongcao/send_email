#!/usr/bin/python
# coding=utf-8

import subprocess


def exec_command_shell(commandfull, cwdpath):
    try:
        result_cmd = subprocess.Popen(commandfull, cwd=cwdpath, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
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
    # cwdpath = "/home/caoweidong/python/practice/"
    # cwdpath = "com/don/monitor_table/"
    cwdpath = "D:\\bigdata\\workspace\\PycharmProjects\\python\\com\\don\\monitor_table\\"

    result_dict = exec_command_shell("python TestPython.py ddd", cwdpath)

    print("stdout = " + str(result_dict["stdout"]))
    print("stderr = " + str(result_dict["stderr"]))
    print("pid = " + str(result_dict["pid"]))
    print("returncode = " + str(result_dict["returncode"]))

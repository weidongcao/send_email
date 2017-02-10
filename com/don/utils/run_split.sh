#!/bin/bash

# 参数说明:
# 	第一个参数:	表名组成的列表
#		可以是一个表名,也可以是多个表名,多个表名之间以逗号分隔,注意不要有空格
#	第二个参数:	开始日期
#		表需要下载的HDFS数据文件所在分区的日期
#	第三个参数:	天数
#		以开始日期开始下载几天的数据
#	第四个参数:	本地保存目录
#	切分后的文件命名以原文件名加下标的形式保存,如原文件名为abc则切分后文件的文件名为abc_0001,abc_0002...

# 功能说明
# 	可以下载一个或多个表的分区数据
#	可以下载多个分区的数据
#	下载的文件会在本地保存目录下根据表名和日期创建新的目录并将文件放在此目录下

# 命令样本:
# ./hdfs_main.py tablename1,tablename2,tablename3 开始日期 天数 本地保存目录

# 切换根目录
cd /data1/shell/split_file/

# 获取当天日期(格式为YYYY-mm-dd)
curdate=`date "+%Y-%m-%d"`

# 日期偏移,0: 不偏移; <0 向过去偏移; >0 向未来偏移 偏移单位:天
dt=`date -d "0 day ${curdate}" +%Y-%m-%d`

# 执行主程序
./hdfs_main.py idl_limao_user_title_agg,idl_limao_info_dim task_id ${dt} 1 /data1/shell/split_file/HDFS_data

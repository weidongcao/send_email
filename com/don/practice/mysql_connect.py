#!/usr/bin/python
# -*- coding: utf8 -*-

import PyMySQL

conn = PyMySQL.connect(host='54.223.85.243',
                       user='dc',
                       passwd='mCdlUmm3thna5ttup',
                       db='datacenter')

cursor = conn.cursor()

cursor.execute("select ds, starttime, endtime, runtime, status from run_info where tablename = 'adl_cid_raw_input'")
# cursor.execute("SELECT VERSION()")

rows = cursor.fetchall()
for row in rows:
    print(row[0] + "\t\t" + row[1] + "\t\t" + row[2] + "\t\t" + str(row[3]) + "\t\t" + row[4])
cursor.close()
conn.close()
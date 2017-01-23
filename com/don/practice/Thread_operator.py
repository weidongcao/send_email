# coding=utf-8

import threading
import time

exitFlag = 0

class HiveThread (threading.Thread):
    def __init__(self, threadID, name, conter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.conter = conter

    def run(self):
        print ("开始线程 : " + self.name)
        print_time(self.name, self.conter, 5)
        print ("退出线程 : " + self.name)
def print_time (threadName, delay, counter):
    while counter:
        if exitFlag:
            threadName.exit()
        time.sleep(delay)
        print("%s: %s" % (threadName, time.ctime(time.time())))
        counter -= 1

## 创建新线程
thread1 = HiveThread(1, "Thread-1", 1)
thread2 = HiveThread(2, "Thread-2", 2)

##开启新线程
thread1.start()
thread2.start()
thread1.join()
thread2.join()
print ("退出主线程")

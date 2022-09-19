
import core.workerGlobal as myglobal
import test_sub1 
import asyncio 
import time
import sys


myglobal.gConfigs.initConfigByFileName("workerConfig.ini") # 读取公用配置文件

print(myglobal.StackTraceBack.getCallerInfo())

sql = "select * from country"


import core.workerGlobal as myglobal
import test_sub1 
import asyncio 
import time




myglobal.gConfigs.initConfigByFileName("workerConfig.ini")
myglobal.gLog2File.info("test info log")
myglobal.gSetupGlobalExceptionCatch("aabbccdd",notify2manager=True)





a = 1/0
time.sleep(500)
raise Exception("bb")
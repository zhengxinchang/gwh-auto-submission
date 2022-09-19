# from configparser import MAX_INTERPOLATION_DEPTH
# import json
from datetime import datetime
from logging import log
from core.sqlite3Manager import logdb as workerlogdb
# from email.policy import default
# from typing import MutableSequence, NewType
# from worker import main
import shutil
import core.workerGlobal as wg
import sys
import os
# from datetime import datetime
import click
from pytz import timezone
# from  core.exportExcel2Table import Excel2Objects
# import core.validation as validation
# from  core.fileManager import fileMangager
# import core.idManager as am
# import core.statFileManager as statfm
# import core.gwhMeta2Mysqldb as gwhm
# import core.sqlite3Manager as sql3m
# import core.eachJobGlobalVars as gvar
import worker as wk

@click.command()
@click.option("-e","--excelfile",required=True,help="excel")
@click.option('-c',"--is_control",is_flag=True,help="is control")
@click.option('-m',"--runmode",required=True,type=click.Choice(['new', 'continue']),help="run mode new submission or continue submission.")
@click.option('-b',"--batchid",required=True,help="batchID, assign it manually in first submission, and reuse it at continue submission.")
@click.option('-d',"--dbtype",required=False,default="test",type=click.Choice(['test', 'prod']),help="config file")
@click.option('-f',"--ftpdir",required=False,default=None,help="specify raw data dir")
@click.option('-a',"--account",required=True,default=None,help="user email account")
@click.option('-y',"--skipcheck",is_flag=True,default=None,help="provide -y will skip manual check of cmd,usually used in automatic system")
def manual(excelfile,is_control,runmode,batchid,dbtype,ftpdir,account,skipcheck):
    jobuid = batchid+":"+account
    # gvar.RuntimeConfig.setAccount(accn=account)
    # gvar.RuntimeConfig.setBatchID(batchid=batchid)
    # gvar.RuntimeConfig.setRunMode(runmode=runmode)
    # gvar.RuntimeConfig.setdbType(dbtype=dbtype)
    # gvar.RuntimeConfig.setJobUID(uid=jobuid)

    # 初始化
    wg.gConfigs.initConfigByFileName("workerConfig.ini") # 读取公用配置文件
    wg.gMailManager.init(batchid=batchid,account=account)
    wg.StartAndEnd.setBatchID(id=batchid)
    wg.gLog2File.setJobuid(jobuid)
    wg.gSetupGlobalExceptionCatch(batchID=batchid,account=account,notify2manager=True) # 捕获全局异常
    wg.StartAndEnd.start(jobuid=jobuid) # 任务运行状态设置为 运行

    # 创建记录
    logdb = workerlogdb()

    if not  logdb.checkIfBatchIDExists(batchid):
        if runmode == "new":
            # 新提交，创建记录
            logdb.addOneBatch(
                INIT_EMAIL_TIME=datetime.now(timezone('Asia/Shanghai')).strftime(wg.gConfigs.get("datetime","emailReceiveDate2DatetimePattern")),
                CURR_EMAIL_TIME= datetime.now(timezone('Asia/Shanghai')).strftime(wg.gConfigs.get("datetime","emailReceiveDate2DatetimePattern")),
                BATCHID= batchid,
                EMAIL=account,
                IS_RUNNING=-1
            )
        else:
            wg.gLog2File.info("BatchID {} not detected in logdb,but you are specify runmode to continue,please check it".format(batchid))
            # wg.gMailManager.mail2manager("BatchID {} not detected in logdb,but you are specify runmode to continue,please check it".format(batchid))
            wg.StartAndEnd.end()
    else:   

        if runmode == "new":
            
            wg.gLog2File.info("BatchID {} exists in logdb,but you are specify runmode to new,please check it".format(batchid))
            # wg.gMailManager.mail2manager("BatchID {} exists in logdb,but you are specify runmode to new,please check it".format(batchid))
            wg.StartAndEnd.end()
            
        else:
                
            is_running  = logdb.getColumnbyBatchID(batchid,"IS_RUNNING")
            if str(is_running) == "0":
                wg.gLog2File.info("Manualy run batch submission {}".format(batchid))
                logdb.updateColumnByBatchID("IS_RUNNING",-1,batchid)
            elif  str(is_running) == "-1":
                wg.gLog2File.info("Manualy run batch submission {}".format(batchid))

            elif str(is_running) == "1":
                wg.gLog2File.info("Batch submission {} is running by Dispather Manager automatic, please waiting for this job done, then you can re-run it manually.")
                # wg.gMailManager.mail2manager("Batch submission {} is running by Dispather Manager automatic, please waiting for this job done, then you can re-run it manually.")
                wg.StartAndEnd.end()
        
            elif str(is_running) == "2":
                wg.gLog2File.info("Batch submission {} is already submitted into GWH, Do not re-run.")
                # wg.gMailManager.mail2manager("Batch submission {} is already submitted into GWH, Do not re-run.")
                wg.StartAndEnd.end()

    batchdir = None
    if dbtype == "test":
        batchdir = os.path.join(wg.gConfigs.get("work_base_dir_test","gwhFileProcess_basedir"),batchid)
    else:
        batchdir = os.path.join(wg.gConfigs.get("work_base_dir_prod","gwhFileProcess_basedir"),batchid)

    try:
        if not os.path.exists(batchdir):
            wg.gLog2File.info("Batch directory {} not exists! creating...".format(batchdir))
            os.mkdir(batchdir)
        # else:
        #     wg.gLog2File.error("Batch directory {} already exists! deleting and recreating...".format(batchdir))
        #     # shutil.rmtree(batchdir)
        #     os.mkdir(batchdir)
    except Exception as e:
        wg.gLog2File.error("Can not handle bachdir {} for batch {}, reason:\n{}".format(batchdir,batchid,str(e)))
        # os.mkdir(self.batchdir)
        # os.mkdir(self.batchdirSubmit)
        # wg.gMailManager.mail2manager("Can not handle bachdir for batch {}, reason:\n{}".format(batchdir,str(e)))
        wg.StartAndEnd.end()
    wk.worker(excelfile=excelfile,is_control=is_control,runmode=runmode,batchid=batchid,dbtype=dbtype,ftpdir=ftpdir,account=account,skipcheck=skipcheck)

if __name__=="__main__":
    manual()


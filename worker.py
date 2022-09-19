from configparser import MAX_INTERPOLATION_DEPTH
import json
# from core.sqlite3Manager import logdb
from email.policy import default
from typing import NewType
import core.workerGlobal as wg
import sys
import os
from datetime import datetime
import click
from  core.exportExcel2Table import Excel2Objects
import core.eachJobGlobalVars as gvar
import core.validation as validation
from  core.fileManager import fileMangager
import core.idManager as am
import core.statFileManager as statfm
import core.gwhMeta2Mysqldb as gwhm
import core.sqlite3Manager as sql3m

@click.command()
@click.option("-e","--excelfile",required=True,help="excel")
@click.option('-c',"--is_control",is_flag=True,help="is control")
@click.option('-m',"--runmode",required=True,type=click.Choice(['new', 'continue']),help="run mode new submission or continue submission")
@click.option('-b',"--batchid",required=True,help="batchID generated in initial submission")
@click.option('-d',"--dbtype",required=False,default="test",type=click.Choice(['test', 'prod']),help="config file")
@click.option('-f',"--ftpdir",required=False,default=None,help="specify raw data dir")
@click.option('-a',"--account",required=True,default=None,help="email account")
@click.option('-y',"--skipcheck",is_flag=True,default=None,help="provide -y will skip manual check of cmd,usually used in automatic system")

def main(excelfile,is_control,runmode,batchid,dbtype,ftpdir,account,skipcheck):
    # 设定jobUID
    worker(excelfile,is_control,runmode,batchid,dbtype,ftpdir,account,skipcheck)


def worker( excelfile,is_control,runmode,batchid,dbtype,ftpdir,account,skipcheck):

    jobuid = batchid+":"+account
    gvar.RuntimeConfig.setAccount(accn=account)
    gvar.RuntimeConfig.setBatchID(batchid=batchid)
    gvar.RuntimeConfig.setRunMode(runmode=runmode)
    gvar.RuntimeConfig.setdbType(dbtype=dbtype)
    gvar.RuntimeConfig.setJobUID(uid=jobuid)
    

    # 初始化
    wg.gConfigs.initConfigByFileName("workerConfig.ini") # 读取公用配置文件
    wg.gMailManager.init(batchid=batchid,account=account)
    wg.StartAndEnd.setBatchID(id=batchid)
    wg.gLog2File.setJobuid(jobuid)
    wg.gSetupGlobalExceptionCatch(batchID=batchid,account=account,notify2manager=True) # 捕获全局异常
    wg.StartAndEnd.start(jobuid=jobuid) # 任务运行状态设置为 运行


    if ftpdir != None:
        if os.path.exists(ftpdir):
            gvar.RuntimeConfig.setFtpDir(ftpdir)
            gvar.RuntimeConfig.set_ftp_from_cmd(True)
        else:
            wg.gLog2File.error("user specified fpt dir {} is not exists!".format(ftpdir))
            # wg.gMailManager.mail2manager("user specified fpt dir {} is not exists!".format(ftpdir))
            wg.StartAndEnd.end()
    else:
        gvar.RuntimeConfig.setFtpDir(wg.gConfigs.get("ftp_dir",'ftp_dir'))
        gvar.RuntimeConfig.set_ftp_from_cmd(False)
    # 根据不同运行模式切换任务目录
    if dbtype == "test":
        gvar.RuntimeConfig.setgwhFileProcessBasedir(wg.gConfigs.get("work_base_dir_test","gwhFileProcess_basedir"))
        gvar.RuntimeConfig.setgwhFileSitesBasedir(wg.gConfigs.get("work_base_dir_test","gwhFileSites_basedir"))
    else:
        gvar.RuntimeConfig.setgwhFileProcessBasedir(wg.gConfigs.get("work_base_dir_prod","gwhFileProcess_basedir"))
        gvar.RuntimeConfig.setgwhFileSitesBasedir(wg.gConfigs.get("work_base_dir_prod","gwhFileSites_basedir"))

    # 修改IS_RUNNING 状态
    
    logdbm = sql3m.logdb()
    IS_RNNING = logdbm.getColumnbyBatchID(gvar.RuntimeConfig.batchID,"IS_RUNNING")
    if str(IS_RNNING) != "-1": 
        # logdbm.updateColumnByBatchID("IS_RUNNING",0,gvar.RuntimeConfig.batchID)
        logdbm.updateColumnByBatchID("IS_RUNNING",1,gvar.RuntimeConfig.batchID)

    # 全局日志记录
    if runmode == "new":
        wg.gLog2File.info("Start new submission")
    else:
        wg.gLog2File.info("Resume previous submission")
    
    # 根据运行模式要求用户double check输入的命令
    if not skipcheck:
        wg.gLog2File.info("Manually run mode, required double check commandlines")
        first_check = input("Please Double Check Your Command, Type [y/Y] If You Comfirm. :")
        if first_check.upper() == "Y":
            pass
        else:
            exit(1)
    else:
        wg.gLog2File.info("Automatic run mode.")
    
    #==========excel export to object=====
    wg.gLog2File.info("Start parse excel file")
    excel= Excel2Objects(excelfile)

    gvar.excelobj.setContact(excel.getContactObjsList())
    gvar.excelobj.setPublication(excel.getPublicationObjsList())
    gvar.excelobj.setAssembly(excel.getAssemblyObjsList())

    if len(gvar.Message.getExcelMessage()) >0 :
        wg.gLog2File.error("Found errors when parsing Excel:\n{}".format("\n".join(gvar.Message.getExcelMessage())))
        wg.gMailManager.mail2user("Found errors when parsing Excel:\n{}".format("\n".join(gvar.Message.getExcelMessage())))
        wg.StartAndEnd.end()

    #==========excel validate=============    
    wg.gLog2File.info("Start validate excel ")
    validation.validate_wether_fa_file_is_unique()
    validation.validate_unique_bigd_accn_and_set_globalvar()
    validation.validate_bioproject_biosmaples()

    #==========ftp validate ==============
    wg.gLog2File.info("Start validate and get information from ftp files ")
    fm = fileMangager()
    # fm.scanftp_file() 
    
    # #==========work env validate =========
    # workspace

    fm.scanftp_file()

    fm.copyftp2workspace()

    # single校验
    fm.getFastaIsSingle()

    #校验md5
    fm.calculateMD5()
    

    #==========生成IDlist=================
    #并且添加is_single信息到IDlist

    am.getBatchGWHAndWGSDict() # 已经update到了gvar.IDlist.getIDlist()中



    #==========占位maxID==================
    
    # logdb默认已经被manager创建了batchID的记录，

    # 生成这个批次的gvar.IDlit

    GWHdbm = gwhm.GWHdbManager()

    # print("rumode:"+gvar.RuntimeConfig.)

    try:
        # 检测 runmode参数，如果是 New
        if gvar.RuntimeConfig.getRunMode() == "new":
            wg.gLog2File.info("Runmode:new detected, system will write ID list to logdb and update maxIDs in Mysql")
            #   将IDlist写入到logdb中
            logdbm.updateColumnByBatchID("IDLIST",json.dumps(gvar.IDlist.getIDlist()),gvar.RuntimeConfig.getBatchID())
            # ，并占位MAXID
            GWHdbm.updateGWHIDandWGSIDtoDB()
            pass
        elif gvar.RuntimeConfig.getRunMode() == "continue" :
            wg.gLog2File.info("Runmode:continue detected, system will try loads IDlist from logdb")
            #  读取是否有IDlist,如果有
            old_IDlist_res = logdbm.getIDLISTbyBatchID(batchid=gvar.RuntimeConfig.batchID)
            old_IDlist = old_IDlist_res[0][0]
            
            #  没有
            if old_IDlist==None:
                wg.gLog2File.info("Runmode:continue detected, IDlist not detected in logdb, writing current IDlist to logdb and update MaxIDs in Mysql")
                #   将IDlist写入到logdb中
                logdbm.updateColumnByBatchID("IDLIST",json.dumps(gvar.IDlist.getIDlist()),gvar.RuntimeConfig.getBatchID())
                # ，并占位MAXID
                GWHdbm.updateGWHIDandWGSIDtoDB()

            else:
                # 读取oldIDlist 
                wg.gLog2File.info("Runmode:continue detected, IDlist detected in logdb")
                old_IDlist_json = json.loads(old_IDlist)
                old_IDlist_datas= old_IDlist_json['datas']
                old_IDlist_metas= old_IDlist_json['metas']

                #比较新旧IDlist，如果数量一致
                if len(old_IDlist_datas) == len(gvar.IDlist.IDlist['datas']):
                    wg.gLog2File.info("Runmode:continue detected,  try to transfer metas and GWHID,WGSID from Old IDlist to new")
                    # 将旧的IDlist中的metas和每个Assembly的GWHID和WGSID转移给新的IDlist
                    for i in range(len(old_IDlist_datas)):
                        gvar.IDlist.IDlist["datas"][i]['GWH'] = old_IDlist_datas[i]['GWH'] 
                        gvar.IDlist.IDlist["datas"][i]['WGS'] = old_IDlist_datas[i]['WGS'] 
                    gvar.IDlist.IDlist["metas"] =  old_IDlist_metas   
                else:
                    #报错
                    wg.gLog2File.error("Runmode:continue detected, Old and New IDlist length are not equal!")
                    wg.gMailManager.mail2user("The assemlby number is not equal to previous submission in this batch, please check it.")
                    wg.StartAndEnd.end()
        else:
            wg.gLog2File.error("Runmode: must be new or continue, {} provided".format(gvar.RuntimeConfig.getRunMode()))
            wg.gMailManager.mail2user("Runmode: must be new or continue, {} provided".format(gvar.RuntimeConfig.getRunMode()))
            wg.StartAndEnd.end()
    except Exception as e:
        wg.gLog2File.error("Can not process and compare IDlist,reason:\n"+str(e))
        # wg.gMailManager.mail2manager("The assemlby number is not equal to previous submission in this batch, please check it.")
        wg.StartAndEnd.end()
    #==========质控=======================
    fm.runQCSehll()
    fm.checkError()

    #==========stat=======================
    stat = statfm.StatShell()
    stat.runStatShell() # stat_res中第二个结果应该是记录stat的信息
    stat.waitDone()
    stat.scanStatsRes()

    #==========入库=======================

    GWHdbm.insertExcelInfoRecords(is_control=is_control)


    #==========通知系统管理员和用户，提交完成
    try:
        batchdir = os.path.join(gvar.RuntimeConfig.getgwhFileProcessBasedir(),gvar.RuntimeConfig.batchID)
        reportfile = os.path.join(batchdir,gvar.RuntimeConfig.batchID+"_"+gvar.RuntimeConfig.account+"_GWHIDAssignment.txt")
        with open(reportfile,'w') as fout:
            for d in gvar.IDlist_with_stats.newIDlist['datas']:
                fout.write("\t".join([
                    d['GWH'],
                    d['WGS'],
                    d['fa'],
                    d['bioproject'],
                    d['biosample'],
                ]) + "\n")
                fout.flush()
        wg.gLog2File.info("creating assignment file successfully!")
        wg.gMailManager.mail2user("Submission is complete!\nAttachment is GWHID and WGSID for each assemlby.",file=reportfile)
        wg.gMailManager.mail2manager("Submission is complete!\nAttachment is GWHID and WGSID for each assemlby.",file=reportfile)
    except Exception as e :
        wg.gLog2File.error("creating assignment file fail!")
#        wg.gMailManager.mail2manager("creating assignment file fail! reason:\n".format(str(e)))
        wg.StartAndEnd.end()
        
    # 修改IS_RUNNING 状态
    logdbm.updateColumnByBatchID("IS_RUNNING",2,gvar.RuntimeConfig.batchID)
    time_on_exit = datetime.now()
    time_delta = time_on_exit - wg.gConfigs.this_run_start_time
    curremaildate_str = logdbm.getColumnbyBatchID(gvar.RuntimeConfig.batchID,"CURR_EMAIL_TIME")
    curremaildate = datetime.strptime(curremaildate_str,wg.gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
    last_process_date = curremaildate + time_delta
    last_process_date_str = last_process_date.strftime(wg.gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
    logdbm.updateColumnByBatchID("PROCESS_LAST_TIME",last_process_date_str,gvar.RuntimeConfig.batchID)

    wg.gLog2File.info("Submission succeed! \nBatchID:{} ; \nemail:{} ; \nbatchdir:{} ; \n ftpdir:{} \nsystem exit!".format(batchid,account,batchdir,ftpdir))
    wg.gMailManager.mail2manager("Submission succeed! \nBatchID:{} ; \nemail:{} ; \nbatchdir:{} ; \n ftpdir:{} \nsystem exit!".format(batchid,account,batchdir,ftpdir))
if __name__ == "__main__":
    main()
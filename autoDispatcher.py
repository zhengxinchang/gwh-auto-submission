
"""
全局捕获异常，并发送邮件，不能有退出的情况
"""

from datetime import datetime
from email import message
import email
from logging import BASIC_FORMAT
import logging
import subprocess
from manager.managerGlobal import mgLog2File as mlogger
from manager.managerGlobal import mgMailManager as mautomail
from manager.managerGlobal import mgConfigs
from manager.managerGlobal import mgSetupGlobalExceptionCatch
from manager.mMail import dispachMialManager
from manager.mException import BreakThisBatchProcessingException
import  manager.fileUtils as fileUtils
import manager.batchIDUtils as batchIDUtils
import time
import os
from manager.msqlite3Manager import logdb
from manager.validationUtils import dispatherValidator
import json
from pytz import timezone
# 
def main():
    print("***************[ GWH smart batch submission system ]**********************")
    currentdir = os.path.dirname(os.path.realpath(__file__))
    mgConfigs.initConfigByFileName("managerConfig.ini")
    mlogger.setJobuid("System Dispatch Manager")
    mautomail.init() # do nothing
    mgSetupGlobalExceptionCatch(notify2manager=True)
    mlogger.info("**************** [ GWH smart batch submission system ] ********************")
    
    logdbm = logdb()
    validator = dispatherValidator()
    # 获得mail的清单

    dbtype = mgConfigs.get("mode","mode")
    
    print("autoDispatcher run as {} mode.".format(dbtype))

    if dbtype == "test":
        workspace_fileprocess_dir = mgConfigs.get("work_base_dir_test","gwhFileProcess_basedir")
        workspace_filesite_dir = mgConfigs.get("work_base_dir_test","gwhFileSites_basedir")
    elif dbtype == "prod" :
        workspace_fileprocess_dir = mgConfigs.get("work_base_dir_prod","gwhFileProcess_basedir")
        workspace_filesite_dir = mgConfigs.get("work_base_dir_prod","gwhFileSites_basedir")

    else:
        print("FETAL ERROR, mode in manager/managerConfig.ini must be one of 'test' or 'prod'")
        exit(1)

    while True:
        dmm = dispachMialManager()
        mlogger.info("Weak Up...")
        print("\n\n")  
        print("*******************"+str(datetime.now().strftime(mgConfigs.get("datetime","emailReceiveDate2DatetimePattern"))) + ": Weak Up...")  

        dmm.fetchMailByLastDate()
        # 获得target邮件清单对象
        dmm.filterMailByLastTimeAndSbuject()
        sorted_New_Sub = dmm.get_sorted_NEW_SUBMISSION()
        sorted_Re_Sub = dmm.get_sorted_RE_SUBMISSION()


        # 循环处理NEW_SUB
        # print("sorted new submission",sorted_New_Sub)
        wait_list_newsub = []
        for oneEmail in sorted_New_Sub:
            try:
                mlogger.info(">>>=======  NEW  =========>>> start processing new submission {}".format(str(oneEmail)))
                
                if oneEmail.get("attach_number") == 0:
                    # mlogger.info("Can not find attachment, sending an email to user...")
                    mautomail.mail2user_with_account("New submisstion email must include one and only one attachment (.xlsx).",oneEmail.get("from"),BatchID="NA")
                    raise BreakThisBatchProcessingException("Found new submission with 0 attachment.")
                elif oneEmail.get("attach_number") >1:
                    # mlogger.info("Can not find attachment, sending an email to user...")
                    mautomail.mail2user_with_account("New submisstion email must include one and only one attachment (.xlsx).",oneEmail.get("from"),BatchID="NA")
                    raise BreakThisBatchProcessingException("Found new submission with >1 attachment.")
                else:
                    
                    msg = oneEmail.get("messageobj")
                    batchID = batchIDUtils.getBatchUID()
                    # print(batchID)
                    workdir = fileUtils.createBatchDirectory(workspace_fileprocess_dir,batchID)
                    # print(workdir)
                    attachment_real_path_list = dmm.retrival_attachment(msg,workdir,batchID)
                    # print(attachment_real_path_list)
                    attachment_real_path = attachment_real_path_list[0]
                    # print(attachment_real_path)
                    attachment_real_md5 = fileUtils.calMD5(attachment=attachment_real_path)
                    if logdbm.checkIfAttachmentMD5Exists(attachment_real_md5):
                        # 存在
                        mlogger.error("Found duplcate NEW SUBMISSION , EXCEL is {}, md5:\n{}".format(str(oneEmail),attachment_real_md5))
                        # mautomail.mail2manager("Found duplcate NEW SUBMISSION , EXCEL is {}, md5:\n{}".format(str(oneEmail),attachment_real_md5))
                        continue
                    else:
                        if not validator.checkExcel(attachment_real_path):
                            mlogger.error("Found Excel {} is not valid. send mail to user...".format(str(oneEmail)))
                            mautomail.mail2manager("Found Excel {} is not valid. send mail to user...".format(str(oneEmail)))     
                            mautomail.mail2user_with_account("Found attachment Excel file {} is not valid. please check it again.".format(str(oneEmail.get("attach_name_list"))),oneEmail.get("from"),BatchID=batchID)
                        else:
                            #所有校验通过，执行worker.py

                            # logdb中增加记录， batchID inittime,currtime,excelmd5,email,isrunning
                            logdbm.addOneBatch(
                                INIT_EMAIL_TIME= oneEmail.get("datetime"),
                                CURR_EMAIL_TIME= oneEmail.get("datetime"),
                                BATCHID=batchID,
                                EXCEL_MD5=attachment_real_md5,
                                IS_RUNNING=0,
                                EMAIL= json.dumps(oneEmail.get("from")) #只保留一个
                            )

                            cmd = "python3  {} -e {} -m {} -b {} -d {} -a {} -y ".format(
                                os.path.join(currentdir,"worker.py"),
                                attachment_real_path,
                                "new",
                                batchID,
                                dbtype,
                                oneEmail.get("from")
                            )
                            mlogger.info("Dispatch NEW_SUBMISSION job with batchID {}".format(batchID))
                            mlogger.info("Exceute {}".format(cmd))
                            np =subprocess.Popen(cmd,shell=True)
                            wait_list_newsub.append(np)
            except Exception as e:
                mlogger.error("can not process new submission {},reason:\n {}".format(str(oneEmail),str(e))) 
                # mautomail.mail2manager("can not process new submission {}, reason:\n{}".format(str(oneEmail),str(e)))
                continue
            finally:
                batchID = None 
            # except BreakThisBatchProcessingException as e :
            #     # 记录那个邮件没有处理完成到日志
            #     mlogger.error("BreakThisBatchProcessingException: can not process new submission {}, reason:\n{}".format(str(oneEmail),str(e)))
            #     mautomail.mail2manager("BreakThisBatchProcessingException: can not process new submission {}, reason:\n{}".format(str(oneEmail),str(e)))
            #     continue
        for xnp in wait_list_newsub:
            xnp.communicate()
        # 循环处理RE_SUB
        
        batchID = None 
        wating_list_resub = []
        for oneReEmail in sorted_Re_Sub:
            try:
                # 解析邮件中的batchID
                msgsubject = oneReEmail.get("subject")
                mlogger.info(">>>========  RE  ========>>> start processing re submission {}".format(str(oneReEmail)))
                
                ReBatchID = msgsubject.lstrip("RE_SUBMISSION:BATCHID=")
                mlogger.info("detect batchID from email: {}".format(ReBatchID))
                
                # msgobj = oneReEmail.get("messageobj")


                # 查询是否在logdb中已经存在
                
                if not logdbm.checkIfBatchIDExists(ReBatchID):
                    mlogger.error("User EMIAL SUBJECT HAS UN-EXISTS BatchID {}, this is not valid. send mail to user...".format(str(ReBatchID)))
                    mautomail.mail2manager("User EMIAL SUBJECT HAS UN-EXISTS BatchID {}, this is not valid. send mail to user...".format(str(ReBatchID)))
                    mautomail.mail2user_with_account("Your RE_SUBMISSION_ID {} passed our BATCHID_PATTERN_CHECK but not store in our database, please check it again.".format(ReBatchID),oneEmail.get("from"),BatchID=ReBatchID) 
                    continue
                else:
                    # 判断是否IS_RUNNING为0 且邮件时间晚于PROCESS_LAST_TIME（PROCESS_LAST_TIME是worker在CURR基础上加上自己运行时间更新的时间）
                    mlogger.info("checkIfBatchIDExists: OK, BatchID {} found in logdb".format(ReBatchID))
                    IS_RUNNING  = logdbm.getColumnbyBatchID(ReBatchID,"IS_RUNNING")
                    LAST_PROCESS_TIME_STR = logdbm.getColumnbyBatchID(ReBatchID,"PROCESS_LAST_TIME")
                    LAST_PROCESS_TIME = datetime.strptime(LAST_PROCESS_TIME_STR,mgConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
                    msgDatetime_STR = oneReEmail.get("datetime")
                    msgDatetime = datetime.strptime(msgDatetime_STR,mgConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
                    
                    # 每个batchID只处理在最后处理期之后的邮件
                    if msgDatetime >LAST_PROCESS_TIME:
                        if str(IS_RUNNING) == "0":

                            mlogger.info("BatchID {} RE submission passed IS_RUNNING and CURR_EMAIL_TIME check".format(ReBatchID))
                            # 更新currenttime 到logdb
                            logdbm.updateColumnByBatchID("CURR_EMAIL_TIME",msgDatetime_STR,ReBatchID)


                            REworkdir = os.path.join(workspace_fileprocess_dir,ReBatchID)
                            if not os.path.exists(REworkdir):
                                os.mkdir(REworkdir)
                            REmsg = oneReEmail.get("messageobj")
                            REattachment_real_path_list = dmm.retrival_attachment(REmsg,REworkdir,ReBatchID)
                            # print(attachment_real_path_list)
                            REattachment_real_path = REattachment_real_path_list[0]


                            REcmd = "python3  {} -e {} -m {} -b {} -d {} -a {} -y ".format(
                                os.path.join(currentdir,"worker.py"),
                                REattachment_real_path,
                                "continue",
                                ReBatchID,
                                dbtype,
                                oneEmail.get("from")
                            )
                            #开始执行

                            
                            mlogger.info("Dispatch RE_SUBMISSION job with batchID {}".format(ReBatchID))
                            mlogger.info("Exceute {}".format(REcmd))
                            p = subprocess.Popen(REcmd,shell=True)
                            wating_list_resub.append(p)

                        elif str(IS_RUNNING) == "-1":
                            mlogger.error("this submission is set to run manulally. mail to  manager. bathcID :{}. IS_RUNNING:{}, EMAIL_DATETIME:{}, logdbLAST_PROCESSED_TIME:{}".format(
                                str(ReBatchID),
                                IS_RUNNING,
                                msgDatetime_STR,
                                LAST_PROCESS_TIME_STR
                                ))
                            mautomail.mail2manager("BatchID {}, is run in manual mode, and user send an email again. please, process it manually.".format(str(ReBatchID)))
                            # mautomail.mail2user_with_account("Your RE_SUBMISSION_ID {} passed our BATCHID_PATTERN_CHECK but not store in our database, please check it again.".format(ReBatchID),oneEmail.get("from"),BatchID=ReBatchID) 

                        else:
                            #报错：
                            mlogger.error("RE_SUB email does not match criteria to run continue submission bathcID :{}. IS_RUNNING:{}, EMAIL_DATETIME:{}, logdbLAST_PROCESSED_TIME:{}".format(
                                str(ReBatchID),
                                IS_RUNNING,
                                msgDatetime_STR,
                                LAST_PROCESS_TIME_STR
                                ))
                    else:
                        mlogger.error("RE_SUB email does not match criteria to run continue submission bathcID :{}.  EMAIL_DATETIME:{}, logdbLAST_PROCESSED_TIME:{}".format(
                                str(ReBatchID),
                               
                                msgDatetime_STR,
                                LAST_PROCESS_TIME_STR
                                ))  
                        
                        # mautomail.mail2manager("RE_SUB email does not match criteria to run continue submission bathcID :{}. IS_RUNNING:{}, EMAIL_DATETIME:{}, logdbLAST_PROCESSED_TIME:{}".format(
                        #     str(ReBatchID),
                        #     IS_RUNNING,
                        #     msgDatetime_STR,
                        #     LAST_PROCESS_TIME_STR
                        #     ))
                        continue

            except Exception as e :
                mlogger.error("can not process RE submission {},reason:\n {}".format(str(oneEmail),str(e))) 
                mautomail.mail2manager("can not process RE submission {}, reason:\n{}".format(str(oneEmail),str(e)))
                continue
            finally:
                ReBatchID =None
        for x in wating_list_resub:
            x.communicate()      
            

        now_time_stamp= dmm.get_NEWEST_EMAIL_TIME_IN_THIS_BATCH()
        mlogger.info("==================== write the datetime fo the last email [ {} ] to LAST_SCAN_TIME.txt".format(now_time_stamp))
        print("==================== write the datetime fo the last email [ {} ] to LAST_SCAN_TIME.txt".format(now_time_stamp))
        with open(os.path.join(currentdir,"LAST_SCAN_TIME.txt"),"w") as xf:
            xf.write(now_time_stamp.strftime(mgConfigs.get("datetime","emailReceiveDate2DatetimePattern")))
        mlogger.info(">>>>>>>>>>>>>>>>>>>>>"+str(datetime.now()) + ": All Emails were Processed, Sleep...")
        print(">>>>>>>>>>>>>>>>>>>>>"+str(datetime.now()) + ": All Emails were Processed, Sleep...")
        time.sleep(int(mgConfigs.get("target_mail","check_interval_sec")))

if __name__== "__main__":

    
    main()
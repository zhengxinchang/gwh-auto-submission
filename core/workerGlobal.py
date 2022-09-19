'''
用于进行全局的变量共享，注意：只用于任务运行程序，不用于任务管理程序。防止冲突和交叉污染。
'''
from manager.managerGlobal import mgMailManager
import os
from collections import OrderedDict
from configparser import ConfigParser
import fcntl
import datetime
from sqlite3.dbapi2 import DatabaseError
import sys
import traceback
from email.mime.text import MIMEText  
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib
import pathlib
import sqlite3
import inspect
import datetime
import traceback
# import core.sqlite3Manager as sqlm
# import core.eachJobGlobalVars as gvar

# 获得当前路径，config文件只读取当前路径下特定文件名
currentdir = os.path.dirname(os.path.realpath(__file__)) # the following 3 lines allow import modules from parent directory.
# step  read configuration file

# 解析全局config文件
class gConfigs():
    confs = ConfigParser()
    this_run_start_time = None
    def initConfigByFileName(filename):
        assert os.path.exists(os.path.join(currentdir,filename))
        gConfigs.confs.read(os.path.join(currentdir,filename))
        gConfigs.this_run_start_time  = datetime.datetime.now()
    def get(db,key):
        try:
            res = gConfigs.confs.get(db,key)
        except:
            res = None
        return(res)    

# 拦截全局异常，输出到日志中，并且发送邮件
# step  global exception handler
def gSetupGlobalExceptionCatch(batchID,account,notify2manager=False):
    def MyExceptionHandler(excType,excValue,traceBack):
        ExceptionMessage = traceback.format_exception(excType, excValue, traceBack)
        jobuid = batchID+":"+account
        Msg = [x.rstrip("\n") for x in ExceptionMessage]
        Msg = " &%& ".join(Msg)
        # Msg = str(StackTraceBack.getCallerInfo()) + " &%& " +Msg
        if notify2manager:
            gLog2File.error(Msg,jobuid)
            gMailManager.auto_mail(gConfigs.get("mail_list","system_manager"),"Notify: BatchID [{}]".format(batchID),"System Crash!,please Check log: \n BatchID: [ {} ]\n Message:\n {}".format(batchID,Msg.replace("&%&","\n"),batchID))
            
        else:
            gLog2File.error(Msg,jobuid)
        # update batchlogdb run status

        logdbm = logdb()
        time_on_exit = datetime.datetime.now()
        time_delta = time_on_exit - gConfigs.this_run_start_time
        curremaildate_str = logdbm.getColumnbyBatchID(batchID,"CURR_EMAIL_TIME")
        curremaildate = datetime.datetime.strptime(curremaildate_str,gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
        last_process_date = curremaildate + time_delta
        last_process_date_str = last_process_date.strftime(gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
        logdbm.updateColumnByBatchID("PROCESS_LAST_TIME",last_process_date_str,batchID)
        gLog2File.info("Set run status to [stop]")
        IS_RNNING = logdbm.getColumnbyBatchID(batchID,"IS_RUNNING")
        if str(IS_RNNING) != "-1":
            logdbm.updateColumnByBatchID("IS_RUNNING",0,batchID)
        exit(1)



    sys.excepthook = MyExceptionHandler



# 全局日志
# step  log2file
class gLog2File():

    jobuid = None
    def setJobuid(uid):
        gLog2File.jobuid=uid

    def __log2file(msg,who="default",level="info",callerfile="-",callerlineno="-"):
        msg = msg.replace("\n"," &%& ")
        logfile = open(gConfigs.get("logger","log_file_prefix"),'a')
        fcntl.flock(logfile, fcntl.LOCK_EX)
        logfile.write("[ {logdate} | worker | {callerfile}:{callerlineno} | {user} | {loglevel} ]=>: {logmsg}\n".format(
            logdate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            user=str(who),
            loglevel = level,
            logmsg = msg,
            callerfile = callerfile,
            callerlineno = str(callerlineno)
        ))
        fcntl.flock(logfile,fcntl.LOCK_UN)
        logfile.close()
    def info(msg,who="default"):
        try:
            previous_frame = inspect.currentframe().f_back 
            (filename, line_number, function_name, lines, index) = inspect.getframeinfo(previous_frame) 
            filename = os.path.basename(filename)
        except:
            filename = "-"
            line_number = "-"

        if who == "default":
            gLog2File.__log2file(msg=msg,who=gLog2File.jobuid,level="info",callerfile=filename,callerlineno=line_number)
        else:
            gLog2File.__log2file(msg=msg,who=who,level="info")
    
    def warn(msg,who="default"):
        # if who == "default":
        #     gLog2File.__log2file(msg=msg,who=gLog2File.jobuid,level="warn")
        # else:
        #     gLog2File.__log2file(msg=msg,who=who,level="warn")
        try:
            previous_frame = inspect.currentframe().f_back 
            (filename, line_number, function_name, lines, index) = inspect.getframeinfo(previous_frame) 
            filename = os.path.basename(filename)
        except:
            filename = "-"
            line_number = "-"

        if who == "default":
            gLog2File.__log2file(msg=msg,who=gLog2File.jobuid,level="warn",callerfile=filename,callerlineno=line_number)
        else:
            gLog2File.__log2file(msg=msg,who=who,level="warn")

    def error(msg,who="default"):
        # if who == "default":
        #     gLog2File.__log2file(msg=msg,who=gLog2File.jobuid,level="error")
        # else:
        #     gLog2File.__log2file(msg=msg,who=who,level="error")
        try:
            previous_frame = inspect.currentframe().f_back 
            (filename, line_number, function_name, lines, index) = inspect.getframeinfo(previous_frame) 
            filename = os.path.basename(filename)
        except:
            filename = "-"
            line_number = "-"

        if who == "default":
            gLog2File.__log2file(msg=msg,who=gLog2File.jobuid,level="error",callerfile=filename,callerlineno=line_number)
        else:
            gLog2File.__log2file(msg=msg,who=who,level="error")

# 每个任务运行的时候在文件夹中的日志
# class logBatchID():


# class StackTraceBack():

#     def getCallerInfo():
#         previous_frame = inspect.currentframe().f_back 
#         (filename, line_number, function_name, lines, index) = inspect.getframeinfo(previous_frame) 

 
#         return ({"callerName":filename, "callerLineNumber":line_number})


# 全局Email
# step  email utils
class gMailManager():

    batchid = None
    account = None
    systemAccn = None

    appendixmessage = """
        
        
        ----HELP INFOMATION-----------------------------------------------------------------------
        When submitting for the first time, "NEW_SUBMISSION" should be filled in the SUBJECT part of the email. The system will automatically run the processing program after receiving the new submission. If the program reports an error, it will return an email containing the error message and attach the BATCHID to mark the batch submission. After the user has modified, send the email again, the SUBJECT part needs to fill in RE_SUBMISSION:BATCHID=xxxx. Replace the xxxx part with the BATCHID returned in the previous email. The system will resume the submission process based on BATCHID. This process may loop multiple times. When all the information is correct and the submission is successful, the system will return a successful submission email with attachments of GWHID and WGSID.
        Notice:
        1. The same excel can only be submitted once NEW_SUBMISSION
        2. The system only detects mails with a specific SUBJECT, other mails will be ignored
        3. The system only detects one attachment of the email 
        ----帮助信息------------------------------------------------------------------------------
        首次提交时，邮件的SUBJECT部分需要填写"NEW_SUBMISSION"。系统接收到新提交后会自动运行处理程序。如果程序报错，则会返回邮件，包含错误信息，同时附上 BATCHID来标记这个批次的提交。用户修改后，再次发送邮件，SUBJECT部分需要填写RE_SUBMISSION:BATCHID=xxxx。将xxxx部分替换为上一封邮件返回的BATCHID。系统会根据BATCHID恢复提交过程。这个过程可能循环多次。当所有信息无误，提交成功后，系统将会返回提交成功邮件，并带有GWHID和WGSID的附件。
        注意：
            1. 同一个excel只能提交一次NEW_SUBMISSION
            2. 系统只检测特定SUBJECT的邮件，其他邮件会被忽略
            3. 系统只检测邮件的一个附件
	
    """

   
    def init(batchid,account):
        gMailManager.batchid =batchid
        gMailManager.account = account
        system_manger = gConfigs.get("mail_list","system_manager")
        if "," in system_manger:
            system_manger= system_manger.split(",")
        gMailManager.systemAccn = system_manger

    
    def auto_mail(address,subject,message,file=None):
        # return(True)
        f_addr=gConfigs.get("sender_mail","username")
        f_pswd=gConfigs.get("sender_mail","password")
        f_smtp=gConfigs.get("sender_mail","smtp")

        if file:
            msg = MIMEMultipart()
            part_text = MIMEText(message)
            msg.attach(part_text)
            part_attach = MIMEApplication(open(file,'rb').read())
            part_attach.add_header('Content-Disposition', 'attachment', filename =pathlib.Path(file).name)
            msg.attach(part_attach) 

        else:
            # msg = MIMEText(msg)
            msg = MIMEText(str(message),'plain','utf-8')

        msg['Subject'] = subject
        msg['From'] = f_addr 
        if isinstance(address,str):
            msg['To'] = address
        elif isinstance(address,list):
            msg['To'] = ",".join(address)
        server = smtplib.SMTP_SSL(f_smtp,465)
        server.set_debuglevel(0)
        server.login(f_addr,f_pswd)
        server.sendmail(f_addr,address,msg.as_string())
        server.quit()

      

    def mail2manager(message,batchid=None,subject="GWH Automatic Notify",file=None):
        message += "\nBatchID [ {} ]\n\nbest\nGWH team".format(str(batchid))
        message = "Dear Manager:\n" +message 
        gMailManager.auto_mail(gMailManager.systemAccn,message=message,subject=subject,file=file)

    def mail2user(message,subject="GWH Automatic FeedBack",file=None):
        message += "\nBatchID:  {}\nSincerely,\nGWH team".format(gMailManager.batchid)
        message = "Dear User " + gMailManager.account + " :\n" +message +"\n\n"+ gMailManager.appendixmessage
        gMailManager.auto_mail(gMailManager.account,message=message,subject=subject,file=file)
        gLog2File.info("Sending an Email to user {}, with content: {}".format(gMailManager.account,message))

# 开始运行worker的时候，在batchlogdb中给某个batchID的任务状态为运行
# 手动退出程序的时候调用end函数，将batchlogdb中给某个batchID的任务状态为非运行
# 注意：异常退出同样需要将batchID的任务状态为非运行

class logdb():
    def __init__(self):
        self.con = sqlite3.connect("SQLITE3_LOGDB_AUTO_SUBMISSION.db")
        self.cur = self.con.cursor()
        # create_table_batchlog = """create table if not exists batch_log (
        # BATCHID	 text not null PRIMARY KEY, 
        # INIT_EMAIL_TIME  text not null, 
        # CURR_EMAIL_TIME text not null, 
        # PROCESS_LAST_TIME text,
        # BIGDACCN text ,
        # BATCHID_DIR	 text , 
        # STATUS	 integer not null default 0, 
        # IDLIST	 text , 
        # FTP_DIR	 text , 
        # PROCESS_DIR	 text ,
        # IS_RUNNING integer not null default 0,
        # EMAIL text not null,
        # EXCEL_MD5 text 
        # );"""
        # self.cur.execute(create_table_batchlog)
        # self.con.commit()
    def logdbStartAndEnd_end(self):
                
        time_on_exit = datetime.datetime.now()
        time_delta = time_on_exit - gConfigs.this_run_start_time
        # curremaildate_str = self.getColumnbyBatchID(StartAndEnd.batchID,"CURR_EMAIL_TIME")
        self.cur.execute("select {} from batch_log WHERE BATCHID = ? ;".format("CURR_EMAIL_TIME"),(StartAndEnd.batchID,))
        results1 = self.cur.fetchall()
        if len(results1) ==1:
            curremaildate_str = results1[0][0]
        else:
            gLog2File.error("workerGlobal: Found CURR_EMIAL_TIME {} results batchID:[{}]".format(StartAndEnd.batchID,len(results1)))
            gMailManager.mail2manager("workerGlobal: Found CURR_EMIAL_TIME  {} results batchID:[{}]".format(StartAndEnd.batchID,len(results1)))
            exit(1)
        curremaildate = datetime.datetime.strptime(curremaildate_str,gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
        last_process_date = curremaildate + time_delta
        last_process_date_str = last_process_date.strftime(gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
        self.cur.execute("update batch_log SET {}  = ? WHERE BATCHID = ? ;".format("PROCESS_LAST_TIME"),(last_process_date_str,StartAndEnd.batchID,))
        
        # self.updateColumnByBatchID("PROCESS_LAST_TIME",last_process_date_str,StartAndEnd.batchID)
        gLog2File.info("Set run status to [stop]")
        # self.updateColumnByBatchID("IS_RUNNING",0,StartAndEnd.batchID)
        self.cur.execute("update batch_log SET {}  = ? WHERE BATCHID = ? ;".format("IS_RUNNING"),(0,StartAndEnd.batchID,))
        print("end job")
        exit(1)


    def updateColumnByBatchID(self,colname,value,batchid):
        
        if colname.strip().strip("'").upper() not in[
            "BATCHID",
            "INIT_EMAIL_TIME" ,
            "CURR_EMAIL_TIME",
            "BIGDACCN",
            "BATCHID_DIR", 
            "STATUS", 
            "IDLIST", 
            "FTP_DIR", 
            "PROCESS_DIR",
            "IS_RUNNING",
            "EMAIL",
            "EXCEL_MD5",
            "PROCESS_LAST_TIME"
        ]:
            gLog2File.error("{} is not a valid colnum name.".format(colname))
            #email
            # gMailManager.mail2manager("{} is not a valid colnum name.".format(colname))
            #stop
            # StartAndEnd.end()
            self.logdbStartAndEnd_end()
            

        sql = "update batch_log SET {}  = ? WHERE BATCHID = ? ;".format(colname)

        try:
            self.cur.execute(sql,(
            value,
            batchid,
        ))
            self.con.commit()
            gLog2File.info("Update {}  to {} in logdb successfully , batchID:[{}]".format(colname,value,batchid))  
            return([True,""])

        except Exception as e:

            gLog2File.error("Can not update Batch info to logdb,rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            self.con.rollback()
            #email
            # gMailManager.mail2manager("Can not update Batch info to logdb,rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            #stop
            self.logdbStartAndEnd_end()

    def getColumnbyBatchID(self,batchid,colname):
        sql = "select {} from batch_log WHERE BATCHID = ? ;".format(colname) #.format(self.__addQuota(batchid))
        try:
            self.cur.execute(sql,(batchid,))
            results = self.cur.fetchall()
            if len(results) ==1:
                return(results[0][0])
            elif len(results) >1:
                gLog2File.error("workerGlobal: Found duplicate results batchID:[{}]".format(batchid))
                # gMailManager.mail2manager("workerGlobal: Found duplicate results batchID:[{}]".format(batchid))
                
                self.logdbStartAndEnd_end()

            elif len(results) == 0:
                gLog2File.error("workerGlobal: No results batchID:[{}]".format(batchid))
                # gMailManager.mail2manager("workerGlobal: No results batchID:[{}]".format(batchid))
                self.logdbStartAndEnd_end()
                
            gLog2File.info("workerGlobal: Query {} from logdb successfully , batchID:[{}]".format(colname,batchid))  
        except Exception as e:
            gLog2File.error("workerGlobal: Can not query  {} by Batchid from logdb, rollback... batchID:[{}]\nresaon:\n{}".format(colname,batchid,str(traceback.format_exc())))
            self.con.rollback()
            # gMailManager.mail2manager("workerGlobal: Can not query  {} by Batchid from logdb, rollback... batchID:[{}]\nresaon:\n{}".format(colname,batchid,str(traceback.format_exc())))
            self.logdbStartAndEnd_end()
        
        

class StartAndEnd():
    batchID = None

    def setBatchID(id):
        StartAndEnd.batchID = id
    def start(jobuid):
        gLog2File.info("Set run status to [running]",jobuid)
        print("start_run...")
    def end():
        logdbm = logdb()
        time_on_exit = datetime.datetime.now()
        time_delta = time_on_exit - gConfigs.this_run_start_time
        curremaildate_str = logdbm.getColumnbyBatchID(StartAndEnd.batchID,"CURR_EMAIL_TIME")
        curremaildate = datetime.datetime.strptime(curremaildate_str,gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
        last_process_date = curremaildate + time_delta
        last_process_date_str = last_process_date.strftime(gConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
        logdbm.updateColumnByBatchID("PROCESS_LAST_TIME",last_process_date_str,StartAndEnd.batchID)
        gLog2File.info("Set run status to [stop]")

        IS_RNNING = logdbm.getColumnbyBatchID(StartAndEnd.batchID,"IS_RUNNING")
        if str(IS_RNNING) != "-1":
            logdbm.updateColumnByBatchID("IS_RUNNING",0,StartAndEnd.batchID)
        gLog2File.error("System Captured errors, please check the log file.",StartAndEnd.batchID)
        print("end job")
        exit(1)

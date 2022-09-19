#!/usr/bin/env python3
from email.message import EmailMessage
from manager.mException import BreakThisBatchProcessingException
import sqlite3

from manager.managerGlobal import mgLog2File as logger
from manager.managerGlobal import mgMailManager as automail
from manager.managerGlobal import mgConfigs
from manager.managerGlobal import datetime


class logdb():
    def __init__(self):
        # 创建log日志数据库
        """
        create table if not exists batchs ( BATCHID	 text not null PRIMARY KEY , INIT_TIME_STAMP text not null, CURR_TIME_STAMP	 text not null, BATCHID_DIR	 text not null, STATUS	 integer not null default 0, IDLIST	 text , ACCN	 text not null, FTP_DIR	 text not null, PROCESS_DIR	 text not null, START_GWHID	 text , END_GWHID text , START_WGSID text , END_WGSID text);
        """

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

    # def __addQuota(self,s):
    #     if s.strip().startswith("'") and s.strip().endswith("'"):
    #         return(s)
    #     elif not (s.strip().startswith("'") and s.strip().endswith("'")):
    #         return("'"+s+"'")
    #     else:
    #         raise Exception("Can not add Quota to String {}".format(s))
    # @staticmethod
    # def addQuota(s):
    #     if s.strip().startswith("'") and s.strip().endswith("'"):
    #         return(s)
    #     elif not (s.strip().startswith("'") and s.strip().endswith("'")):
    #         return("'"+s+"'")
    #     else:
    #         raise Exception("Can not add Quota to String {}".format(s))        

    def myStartAndEnd(self):
        '''自定义退出逻辑，防止循环依赖'''
        # logger.error("logdb crash..")
        # automail.mail2manager("")
        # exit(1)
        automail.mail2manager("System captured errors, please check the log file.")
        exit(1)
        

    def addOneBatch(self,INIT_EMAIL_TIME =None, 
                    CURR_EMAIL_TIME=None,
                    PROCESS_LAST_TIME=None,
                    BATCHID=None,
                    BIGDACCN=None,
                    BATCHID_DIR=None,
                    STATUS=0,
                    IDLIST=None,
                    FTP_DIR=None,
                    PROCESS_DIR=None,
                    IS_RUNNING =None,
                    EMAIL = None,
                    EXCEL_MD5=None
                    ):
        sql = """insert into batch_log (
            INIT_EMAIL_TIME ,
            CURR_EMAIL_TIME, 
            BATCHID, 
            BIGDACCN,
            BATCHID_DIR, 
            STATUS, 
            IDLIST, 
            FTP_DIR, 
            PROCESS_DIR,
            IS_RUNNING,
            EMAIL,
            EXCEL_MD5,
            PROCESS_LAST_TIME ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);"""
        
        parmas = (
            INIT_EMAIL_TIME  ,
            CURR_EMAIL_TIME ,
            BATCHID ,
            BIGDACCN,
            BATCHID_DIR ,
            STATUS ,
            IDLIST ,
            FTP_DIR ,
            PROCESS_DIR ,
            IS_RUNNING,
            EMAIL,
            EXCEL_MD5,
            PROCESS_LAST_TIME
        )
        
        # try:
        #     uniq= self.checkIfUniqueBatchID(BATCHID)
        #     if not uniq[0]:
        #         return([False,uniq[1]])
                
        # except Exception as e:
        #     logging.info("Found duplicate ")
        #     return([False,str(e)])

        # if uniq[1]['statu'] != "notexists":
        #     return([False,"batchid {} already in logdb".format(BATCHID)])             

        try:
            self.cur.execute(sql,parmas)
            self.con.commit()
            logger.info("Add New Batch info to logdb successfully , batchID:[{}]".format(BATCHID))  
            
            return([True,"add logdb succeed! batchid {}".format(BATCHID)])
            
        except Exception as e:
            
            logger.error("Can not add Batch info to logdb,rollback... batchID:[{}]\nresaon:\n{}".format(BATCHID,str(e)))
            #email
            # automail.mail2manager("Can not add Batch info to logdb,rollback... batchID:[{}]\nresaon:\n{}".format(BATCHID,str(e)))
            #stop
            self.con.rollback()
            self.myStartAndEnd()
            return([False,str(e)])


    def updateColumnByBatchID(self,colname,value,batchid):
        
        if colname.strip().strip("'").upper() not in [
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

    

            logger.error("{} is not a valid colnum name.".format(colname))
            #email
            # automail.mail2manager("{} is not a valid colnum name.".format(colname))
            #stop
            self.myStartAndEnd()
            

        sql = "update batch_log SET {}  = ? WHERE BATCHID = ? ;".format(colname)

        try:
            self.cur.execute(sql,(
            value,
            batchid,
        ))
            self.con.commit()
            logger.info("Update {}  to {} in logdb successfully , batchID:[{}]".format(colname,value,batchid))  
            return([True,""])

        except Exception as e:

            logger.error("Can not update Batch info to logdb,rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            self.con.rollback()
            #email
            # automail.mail2manager("Can not update Batch info to logdb,rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            #stop
            self.myStartAndEnd()
            return([False,str(e)]) 
        
    def getOneRecordByBIGDACCN(self,bigdaccn):
        sql = "select * from batch_log WHERE BIGDACCN = ?;"
        try:
            self.cur.execute(sql,(bigdaccn,))
            results = self.cur.fetchall()
            logger.info("Query record in logdb successfully , BIGDACCN:[{}]".format(bigdaccn)) 
            return([True,results])
        except Exception as e:
            
            logger.error("Can not query Batch info from logdb by bigdaccn,rollback... BIGDACCN:[{}]\nresaon:\n{}".format(bigdaccn,str(e)))
            self.con.rollback()
            #email
            # automail.mail2manager("Can not query Batch info from logdb by bigdaccn,rollback... BIGDACCN:[{}]\nresaon:\n{}".format(bigdaccn,str(e)))
            #stop
            self.con.rollback()
            self.myStartAndEnd()
            return([False,str(e)])
    
    def getOneRecordByBatchID(self,batchid):
        sql = "select * from batch_log WHERE BATCHID = ?;"
        try:
            self.cur.execute(sql,(batchid,))
            results = self.cur.fetchall()
            logger.info("Query record in logdb successfully , BatchID:[{}]".format(batchid)) 
            return([True,results])
        except Exception as e:
            
            logger.error("Can not Query Batch info from logdb,rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            self.con.rollback()
            #email
            # automail.mail2manager("Can not Query Batch info from logdb,rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            #stop
            self.myStartAndEnd()

            return([False,str(e)])

    def checkIfBatchIDExists(self,batchid):
        sql = "select * from batch_log WHERE BATCHID = ?;"
        # print(sql)
        try:
            self.cur.execute(sql,(batchid,))
            results = self.cur.fetchall()
            print(results)
            if len(results) ==1:
                return(True)
            elif len(results) == 0:
                return(False)
            else:
                logger.error("Found duplicate BatchID {} in logdb".format(batchid))
                # automail.mail2manager("Found duplicate BatchID {} in logdb".format(batchid))    

        except Exception as e:
            logger.error("Query wether BatchID {} is exists failed reason:\n{}".format(batchid,str(e)))
            # automail.mail2manager("Query wether BatchID {} is exists failed reason:\n{}".format(batchid,str(e)))
            return(False)
    def checkIfAttachmentMD5Exists(self,md5):
            # print(md5)
            logger.info("MD5 of excel is {}".format(md5))
            
            sql2 = "select * from batch_log;"
            self.cur.execute(sql2)
            res2 = self.cur.fetchall()
            print("all ting from batch_log ",res2)

            sql = "select * from batch_log WHERE EXCEL_MD5 = ?;"
            # print(sql)
            try:
                self.cur.execute(sql,(md5.strip(),))
                results = self.cur.fetchall()
                # print(results)
                if len(results) ==1:
                    return(True)
                elif len(results) == 0:
                    return(False)
                else:
                    logger.error("Found duplicate md5 {} in logdb".format(md5))
                    # automail.mail2manager("Found duplicate md5 {} in logdb".format(md5))    
                    raise BreakThisBatchProcessingException("Found duplicate md5 {} in logdb".format(md5)) 
            except Exception as e:
                logger.error("Query wether md5 {} is exists failed reason:\n{}".format(md5,str(e)))
                # automail.mail2manager("Query wether md5 {} is exists failed reason:\n{}".format(md5,str(e)))
                return False

    def getColumnbyBatchID(self,batchid,colname):
        sql = "select {} from batch_log WHERE BATCHID = ? ;".format(colname) #.format(self.__addQuota(batchid))
        try:
            self.cur.execute(sql,(batchid,))
            results = self.cur.fetchall()
            if len(results) ==1:
                return(results[0][0])
            elif len(results) >1:
                logger.error("Found duplicate results batchID:[{}]".format(batchid))
                automail.mail2manager("Found duplicate results batchID:[{}]".format(batchid))
            elif len(results) == 0:
                logger.error("No results batchID:[{}]".format(batchid))
                automail.mail2manager("No results batchID:[{}]".format(batchid))
            logger.info("Query {} from logdb successfully , batchID:[{}]".format(colname,batchid))  
        except Exception as e:
            logger.error("Can not query  {} by Batchid from logdb, rollback... batchID:[{}]\nresaon:\n{}".format(colname,batchid,str(e)))
            self.con.rollback()
            automail.mail2manager("Can not query  {} by Batchid from logdb, rollback... batchID:[{}]\nresaon:\n{}".format(colname,batchid,str(e)))
            
              


    # def checkIfUniqueBatchID(self,batchid):
    #     sql = "select * from batch_log WHERE BATCHID = {};".format(self.__addQuota(batchid))
    #     try:
    #         self.cur.execute(sql)
    #         results = self.cur.fetchall()
    #         if len(results) ==1:
    #             return([True,{"statu":"unique","value":results}])
    #         elif len(results) == 0:
    #             return([True,{"statu":"notexists","value":results}])
    #         else:
    #             return([True,{"statu":"multiple","value":results}])

    #     except Exception as e:
    #         self.con.rollback()
    #         return([False,str(e)])        

    def getIDLISTbyBatchID(self,batchid):
        sql = "select IDLIST from batch_log WHERE BATCHID = ? ;" #.format(self.__addQuota(batchid))
        try:
            self.cur.execute(sql,(batchid,))
            results = self.cur.fetchall()
            if len(results) ==1:
                return(results)
            elif len(results) >1:
                logger.error("Found duplicate results batchID:[{}]".format(batchid))
                # automail.mail2manager("Found duplicate results batchID:[{}]".format(batchid))
                self.myStartAndEnd()

            elif len(results) == 0:
                logger.error("No results batchID:[{}]".format(batchid))
                # automail.mail2manager("No results batchID:[{}]".format(batchid))
                self.myStartAndEnd()
                
            logger.info("Query IDlist from logdb successfully , batchID:[{}]".format(batchid))  
        except Exception as e:
            logger.error("Can not query  ID list by Batchid from logdb, rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            self.con.rollback()
            # automail.mail2manager("Can not query  ID list by Batchid from logdb, rollback... batchID:[{}]\nresaon:\n{}".format(batchid,str(e)))
            self.myStartAndEnd()
            return([False,str(e)])

    

if __name__ == "__main__":

    pass
    # testdb = logdb()
    # gConfigs.initConfigByFileName("workerConfig.ini") # 读取公用配置文件
    # automail.init(batchid="CC",account="zhengxinchang1993@163.com")
    # logger.setJobuid("xxxxxxyyyyzzzz")

    # a = testdb.addOneBatch(
    #     INIT_TIME_STAMP= "aa",
    #     CURR_TIME_STAMP= "bb",
    #     BATCHID= "CC",
    #     BIGDACCN="AXX",
    #     BATCHID_DIR= "DD",
    #     FTP_DIR= "EE",
    #     PROCESS_DIR= "FF",
    #     IS_RUNNING= 1,
    #     EMAIL="XXXYY",
    #     STATUS= 1
    # )
   
    # print(testdb.getIDLISTbyBatchID("CC"))
    # # print(json.loads(testdb.getIDLISTbyBatchID("CC")))
    # print(testdb.getIDLISTbyBatchID("CC")[0][0])
    # print(testdb.getOneRecordByBIGDACCN("bb"))
    # print(testdb.updateColumnByBatchID("CURR_TIME_STAMP","SSSSSSxxxxxxxxxxxxxxxxxxxSSSSSSSSSSS","CC"))


import imaplib
import email
from email.parser import Parser
from email.header import decode_header
import email.utils #专门处理地址的模块

# from datetime import date, datetime
import datetime
from manager.managerGlobal import mgConfigs
from manager.managerGlobal import mgLog2File as mlogger
from manager.managerGlobal import mgMailManager as mautomail
from manager.mException import BreakThisBatchProcessingException
from manager.managerGlobal  import mgConfigs
# from managerGlobal import mgConfigs
# from managerGlobal import mgLog2File as mlogger
# from managerGlobal import mgMailManager as mautomail
# from mException import BreakThisBatchProcessingException
# from  managerGlobal  import mgConfigs
from dateutil.parser import parse as dateparse
import os
import manager.validationUtils as validationUtils
import base64
import functools


class dispachMialManager():

    def __init__(self):
        """这些应该放到外边去，这个类仅仅执行一次任务中的检测邮件任务，不会导致数据分析"""
        # currentdir = os.path.dirname(os.path.realpath(__file__)) # the following 3 lines allow import modules from parent directory.
        # timestamp_path = os.path.join(currentdir,"LAST_CHECK_MAIL_TIME_STAMP.txt")
        # if not  os.path.exists(timestamp_path):
        #     with open("LAST_CHECK_MAIL_TIME_STAMP.txt",'w') as inf:
        #         inf.write(datetime.datetime.now().strftime("%Y-%m-%d/%H:%M:%S"))
        #         print("LAST_CHECK_MAIL_TIME_STAMP not found")
        # datetime.timedelta(seconds=mgConfigs.get("target_mail","check_interval_sec"))

        
        # self.LAST_TIME="5:00:23"
        # self.LAST_DATETIME_STR= "Mon, 06 Sep 2021 08:14:28 +0800"

        currentdir = os.path.dirname(os.path.realpath(__file__)) # the following 3 lines allow import modules from parent directory.
        parentdir = os.path.dirname(currentdir)

        with open(os.path.join(parentdir,"LAST_SCAN_TIME.txt")) as f:
            self.LAST_DATETIME_STR=f.readline()

        self.LAST_DATETIME = datetime.datetime.strptime(self.LAST_DATETIME_STR,mgConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
        self.LAST_DATE_STR=self.LAST_DATETIME.strftime("%d-%b-%Y")
        self.timeinterval = '(since "'+self.LAST_DATE_STR+'")'
        self.mailListFetchedByLastDate = [] #存储fetchMailByLastDate函数返回的数据
        self.FilterdMailList_NEW_SUB = []
        self.FilterdMailList_RE_SUB = []
        self.validate = validationUtils.dispatherValidator()

        self.NEWEST_EMAIL_TIME_IN_THIS_BATCH = None 

    def fetchMailByLastDate(self):
        """检查邮箱，获得所有符合date的邮件
            输入：None
            输入：一个列表容器，包含多个邮件字典，包含Message对象
        """
        mlogger.info("start to fetch email from the target email server {} with username {}".format(mgConfigs.get("target_mail","host"),mgConfigs.get("target_mail","username")))
        try:
            # print(mgConfigs.get("target_mail","host"))
            # print(mgConfigs.get("target_mail","port"))
            imapclient = imaplib.IMAP4_SSL(port=mgConfigs.get("target_mail","port"),host=mgConfigs.get("target_mail","host"))
            imapclient.login(user=mgConfigs.get("target_mail","username"),password=mgConfigs.get("target_mail","password"))
            imapclient.select("INBOX",readonly=True)
            xtype,xdata = imapclient.search(None ,self.timeinterval)
            mlist = xdata[0].decode("utf-8").split()
            # print(mlist)
            mlogger.info("fetchMailByLastDate found {} emails since {}".format(str(len(mlist)), self.LAST_DATE_STR))
            for m in mlist:
                # print("xxxxx")
                # print(m)
                ftype, fdatas = imapclient.fetch(m, '(RFC822)')
                tmpMessage = email.message_from_bytes(fdatas[0][1])
                self.mailListFetchedByLastDate.append(tmpMessage)
            return(True)

        except Exception as e:
            msg = "Can not fetch data from target email server {}, reason:\n{} ".format(mgConfigs.get("target_mail","host"),str(e))
            mautomail.mail2manager(msg)
            raise BreakThisBatchProcessingException(msg)


    def filterMailByLastTimeAndSbuject(self):
        """
        对获得的指定日期的邮件进行进一步过滤:
        1. 晚于LAST时间
        2. subject符合规范
        返回的数据：
        [
            {
                messageobj:message
                subject:str
                submissioin_type:new|continue
                attach_number:int
                datetime: msgReceivedTimeDateTime
            },
        ]
        """
        mlogger.info("filterMailByLastTimeAndSbuject start filter submission emails...")
        hits = 0
        processed = 0
        # print(self.mailListFetchedByLastDate)
        for msg in self.mailListFetchedByLastDate:
            # print(msg.get("RECEIVED"))
            # print(msg.get("SUBJECT"))

            try:
                msgReceivedTimeString =  [ y.strip() for y in msg.get("RECEIVED").split(";")][-1]
                msgReceivedTimeString = msgReceivedTimeString.strip()
            except:
                mlogger.warn("Can not parse RECEIVE attr from message, subject {}: received: {}".format(msg.get("SUBJECT"),msg.get("RECEIVED")))
                continue
            # print(msgReceivedTimeString)
            # msgReceivedTimeDateTime = datetime.datetime.strptime(msgReceivedTimeString,"%a, %d %b %Y %H:%M:%S %z (CST)")
            try:
                # print("datetime original : {}".format(msgReceivedTimeString))
                msgReceivedTimeString = msgReceivedTimeString.replace("\n"," ")
                # print("datetime string clean : {}".format(msgReceivedTimeString))
                msgReceivedTimeDateTime = dateparse(msgReceivedTimeString)
                self.NEWEST_EMAIL_TIME_IN_THIS_BATCH = msgReceivedTimeDateTime
                # print("datetime parsed by dateutil: {}".format(msgReceivedTimeDateTime))
            except:
                continue
            # print(self.LAST_DATETIME)
            # print(msgReceivedTimeDateTime)


            from_email_tuple = email.utils.parseaddr(msg.get("From"))
            
            from_email_list = []
            for k in from_email_tuple:
                if self.validate.checkEmail(k.strip()):
                    from_email_list.append(k)


            if msgReceivedTimeDateTime > self.LAST_DATETIME:
                hits += 1
                subject = msg.get("subject")
                # print(subject)
                if self.validate.checkSubject(subject):
                    """验证通过，正式添加一个对象到容器"""
                    tmpMessage = {}
                    tmpMessage['messageobj']=msg
                    tmpMessage['subject'] = subject
                    attach_name_list = self.get_attachment_list(msg)
                    tmpMessage['attach_name_list'] = attach_name_list
                    tmpMessage['attach_number'] = len(attach_name_list)
                    tmpMessage['datetime'] = msgReceivedTimeDateTime.strftime(mgConfigs.get("datetime","emailReceiveDate2DatetimePattern"))
                    tmpMessage['from'] = from_email_list[0]
                    if subject.startswith("NEW_SUBMISSION"):
                        tmpMessage['submissioin_type']="new"
                    elif     subject.startswith("RE_SUBMISSION:BATCHID"):
                        tmpMessage['submissioin_type']="continue"
                    if tmpMessage.get('submissioin_type') == "new":
                        self.FilterdMailList_NEW_SUB.append(tmpMessage)
                    else:
                        self.FilterdMailList_RE_SUB.append(tmpMessage)
        mlogger.info("filterMailByLastTimeAndSbuject successfully process {} targeted emails. new submission is {}, re-submission is {}".format(str(hits),str(len(self.FilterdMailList_NEW_SUB)),str(len(self.FilterdMailList_RE_SUB))))
        print("filterMailByLastTimeAndSbuject successfully process {} targeted emails. new submission is {}, re-submission is {}".format(str(hits),str(len(self.FilterdMailList_NEW_SUB)),str(len(self.FilterdMailList_RE_SUB))))
    def get_sorted_NEW_SUBMISSION(self):
        """
        将邮件中的NEW_SUBMISSION，以时间从旧到新进行排序,注册到gvar
        """
        # print("start to sort new submission")
        def sortMessageFromOldToNew(x,y):
            x_datetime = x.get("datetime")
            y_datetime = y.get("datetime")
            return(x_datetime<y_datetime)
        self.FilterdMailList_NEW_SUB.sort(key=functools.cmp_to_key(sortMessageFromOldToNew))
        return(self.FilterdMailList_NEW_SUB)
    def get_sorted_RE_SUBMISSION(self):
        """
        将邮件中的RE_SUBMISSION,  以时间从新到旧进行排序,注册到gvar
        """
        def sortMessageFromNewToOld(x,y):
            x_datetime = x.get("datetime")  
            y_datetime = y.get("datetime")
            return(x_datetime<y_datetime)
        self.FilterdMailList_RE_SUB.sort(key=functools.cmp_to_key(sortMessageFromNewToOld))
        return(self.FilterdMailList_RE_SUB)

    def get_NEWEST_EMAIL_TIME_IN_THIS_BATCH(self,):
        return(self.NEWEST_EMAIL_TIME_IN_THIS_BATCH )
         
    def __decode_str(self,s):
        """https://www.jianshu.com/p/544a35bc8c92"""
        value, charset = decode_header(s)[0]
        if charset:
            value = value.decode(charset)
        return value

    def __guess_charset(self,msg):
        """https://www.jianshu.com/p/544a35bc8c92"""
        charset = msg.get_charset()
        if charset is None:
            content_type = msg.get('Content-Type', '').lower()
            pos = content_type.find('charset=')
            if pos >= 0:
                charset = content_type[pos+8:].strip()
        return charset

    def retrival_attachment(self,msg,savepath,batchID) :
        """https://www.jianshu.com/p/544a35bc8c92"""

        count = 0
        filepathlist = []
        try:
            for p in msg.walk():
                filename=p.get_filename()
                if filename!=None:#如果存在附件
                    count += 1
                    filename = self.__decode_str(filename)#获取的文件是乱码名称，通过一开始定义的函数解码
                    # print(">>>"+str(filename))
                    ddata = p.get_payload(decode = True)#取出文件正文内容
                    #此处可以自己定义文件保存位置
                    truefilename = batchID+"_"+str(count)+".xlsx"
                    fpath = os.path.join(savepath,truefilename)
                    filepathlist.append(fpath)
                    with open(fpath, 'wb') as f:
                        f.write(ddata)
                        f.close()
                    mlogger.info('file {} download from mail server, saved at {} with name {}'.format(filename,savepath,truefilename))
            return(filepathlist)
        except Exception as e :
            mlogger.error("Can not retrival attachment {} batchID :{}".format(filename,batchID))
            mautomail.mail2manager("Can not retrival attachment {} batchID :{}".format(filename,batchID))
    def get_attachment_list(self,msg) :
        attach_name_list= []
        for p in msg.walk():
            filename=p.get_filename()
            if filename!=None:#如果存在附件
                attach_name_list.append(self.sender_decode(filename))
        return(attach_name_list)

 
    def sender_decode(self,sender):
        return(sender)
        parsed_string = sender.split("?")

        decoded = base64.b64decode(parsed_string[3]).decode(parsed_string[1], "ignore")
        return decoded
   

    
    
    # try:
    #     timeinterval = '(since "'+(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%d-%b-%Y")+'")'
    #     imapclient = imaplib.IMAP4_SSL(port=mgConfigs.get("target_mail","port"),host=mgConfigs.get("target_mail","host"))
    #     imapclient.login(user=mgConfigs.get("target_mail","username"),password=mgConfigs.get("target_mail","password"))
    #     imapclient.select(readonly=True)
    #     type,data = imapclient.search(None ,'ALL')
    #     print(data)
    #     msgList = data[0].split()
    #     last = msgList[len(msgList) - 1]
    #     fetch_type,fetch_data = imapclient.fetch(last,"(RFC822)")
    #     text = fetch_data[0][1]
    #     message = email.message_from_string(text)
    #     print(message)
    # except Exception as e:
    #     mlogger.error("Can not fetch data from target email {}, reason:\n{} ".format(mgConfigs.get("target_mail","host"),str(e)))
    #     mautomail.mail2manager("Can not fetch data from target email {}, reason:\n{} ".format(mgConfigs.get("target_mail","host"),str(e)))


if __name__=="__main__":
    pass
    

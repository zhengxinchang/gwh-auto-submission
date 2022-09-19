'''每个worker运行的时候分别初始化的全局变量，不同的任务之间不共享'''

# step  batchID
from core.workerGlobal import gLog2File as logger
import json

class RuntimeConfig():
    batchID=None
    runmode=None
    account = None
    dbtype = None
    jobuid = None 
    gwhFileProcessBasedir=None
    gwhFileSitesBasedir=None
    ftpdir = None
    ftp_from_cmd=None

    def set_ftp_from_cmd(flag):
        RuntimeConfig.ftp_from_cmd = flag
    def get_ftp_from_cmd():
        return(RuntimeConfig.ftp_from_cmd)
        
    def setFtpDir(d):
        RuntimeConfig.ftpdir = d

    def getFtpDir():
        return(RuntimeConfig.ftpdir)

    def setgwhFileProcessBasedir(p):
        RuntimeConfig.gwhFileProcessBasedir =p
    def getgwhFileProcessBasedir():
        return(RuntimeConfig.gwhFileProcessBasedir)
    def setgwhFileSitesBasedir(p):
        RuntimeConfig.gwhFileSitesBasedir =p
    def getgwhFileSitesBasedir():
        return(RuntimeConfig.gwhFileSitesBasedir) 
           
    def setBatchID(batchid):
        RuntimeConfig.batchID = batchid
    def getBatchID():
        return(RuntimeConfig.batchID)
    def setRunMode(runmode):
        RuntimeConfig.runmode = runmode
    def getRunMode():
        return(RuntimeConfig.runmode)   

    def setAccount(accn):
        RuntimeConfig.account = accn
    def getAccount():
        return(RuntimeConfig.account)   
    def setdbType(dbtype):
        RuntimeConfig.dbtype = dbtype
    def getdbType():
        return(RuntimeConfig.dbtype)   
    def setJobUID(uid):
        RuntimeConfig.jobuid = uid
    def getJobUID():
        return(RuntimeConfig.jobuid)



# step  excel objects
class excelobj():
    '''
    以下三个对象均是这种格式
    [
        {

        },{

        }
    ]
    '''
    contact = None
    
    publication = None
    assembly = None

    def setContact(contact):
        logger.info("setup contact "+ str(contact))
        excelobj.contact = contact
    def getContact():
        return(excelobj.contact)

    def setPublication(publication):
        logger.info("setup publication "+str(publication))
        excelobj.publication = publication
    def getPublication():
        return(excelobj.publication)

    def setAssembly(assembly):
        # print(assembly)
        logger.info("setup assembly "+ str(assembly))
        excelobj.assembly = assembly
    def getAssembly():
        return(excelobj.assembly)        

# step  file objects
class fileobject():
    isSingleDict = None
    def setIsSingleDict (issinglelist):
        logger.info("is Single information "+ str(issinglelist))
        fileobject.isSingleDict = issinglelist
    def getIsSingleDict ():
        return(fileobject.isSingleDict)

# step  IDlist
class IDlist():

    IDlist = None
    def setIDlist(idl):
        IDlist.IDlist = idl
    def getIDlist():
        return(IDlist.IDlist)


class IDlist_with_stats():
    newIDlist = None
    def setIDlist(idl):
        IDlist_with_stats.newIDlist = idl
    def getIDlist():
        return(IDlist_with_stats.newIDlist)
# step errorMessage
class Message():
    excelMessage = []
    fileMessage = []

    def addExcelMessage(msg):
        Message.excelMessage.append(msg)

    def getExcelMessage():
        return(Message.excelMessage)

class bigdAccn():
    accn = None
    def setBigdAccn(accn):
        bigdAccn.accn = accn
    def getBigdAccn():
        return(bigdAccn.accn)
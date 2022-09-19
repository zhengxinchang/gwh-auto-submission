
from manager.managerGlobal import mgLog2File as mlogger


class NotifyManager(Exception):

    def __init__(self,msg,info):
        self.msg = msg
        mlogger.error("An exception occured during processing email.")

    def __str__(self):
        return self.msg

class NotifyUserAndManager(Exception):
    
    def __init__(self,msg,info):
        self.msg = msg
        mlogger.error("An exception occured during processing email.")

    def __str__(self):
        return self.msg

class BreakThisBatchProcessingException(Exception):
    
    def __init__(self,msg):
        self.msg = msg
        mlogger.error(msg)

    def __str__(self):
        return self.msg

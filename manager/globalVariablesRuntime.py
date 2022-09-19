"""
存储每次定时任务运行的时候的共有变量。每次定时任务的时候都要进行重置

"""



class  Runtimegvar():

    mailListFetchedByLastDate = None

    # mailList_NEW_SUB = None

    # mailList_RE_SUB = None
    
    def set_mailListFetchedByLastDate(obj):
        Runtimegvar.mailListFetchedByLastDate = obj
    def get_mailListFetchedByLastDate():
        return(Runtimegvar.mailListFetchedByLastDate)

    # def set_mailList_NEW_SUB(mlist):
    #     Runtimegvar.mailList_NEW_SUB = mlist
    # def get_mailList_NEW_SUB():
    #     return(Runtimegvar.mailList_NEW_SUB) 

    # def set_mailList_RE_SUB(mlist):
    #     Runtimegvar.mailList_RE_SUB = mlist
    # def get_mailList_RE_SUB():
    #     return(Runtimegvar.mailList_RE_SUB)     
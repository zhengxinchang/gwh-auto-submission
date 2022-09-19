
import os 
from manager.mException import BreakThisBatchProcessingException
from manager.managerGlobal import mgLog2File as mlogger
import subprocess
import re
def createBatchDirectory(basepath,batchid):
    assert os.path.exists(basepath)
    target_dir = os.path.join(basepath,batchid)

    try:
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
        return(target_dir)
    except Exception as e:
        mm = "Can not create batch directory {} for batchid:{},reason:\n{}".format(target_dir,batchid),str(e)
        raise BreakThisBatchProcessingException(mm)
def calMD5(attachment):
    mlogger.info("Calculating MD5...")
    cmd = "md5sum " + attachment  # + "> "  + os.path.join(self.batchdirSubmit,"md5sum.txt")  
    res = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out=res.stdout.readlines()
    a = out[0]
    md5,longfile = re.split(" +",a.decode().strip())
    mlogger.info("calcualte MD5 for attachment {} ,MD5 is {}".format(attachment,str(md5)))
    return(md5.strip())
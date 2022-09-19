
import uuid
import datetime
def getBatchUID(key=None):
    currtime = datetime.datetime.now().strftime("%Y_%m_%d_%H")
    prefix = "batch_" + currtime + "_"
    if key==None:
        return(prefix + str(uuid.uuid4()))

    else:
        return prefix + str(uuid.uuid3(uuid.NAMESPACE_DNS,str(key)))

if __name__=="__main__":
    print(getBatchUID())
    print(getBatchUID("xx"))
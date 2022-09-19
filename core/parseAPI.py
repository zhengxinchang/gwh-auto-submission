#!/usr/bin/env python3
'''
模块用来解析URL的API数据，返回对应的字典
从config中读取base RUL

添加api：
config中添加API_XXX开头的键值对
在class API中添加对应的函数

'''

# import requests
import requests
import os
import logging 
from collections import OrderedDict
import json
import sys
import ast # better eval
import json 
# import Mysql
import core.mysqlManager as Mysql
import http.client
from core.workerGlobal import gConfigs
from  core.workerGlobal import gLog2File as logger
import core.eachJobGlobalVars as gvar

http.client._MAXHEADERS = 1000

class API():

    def __init__(self):

        # self.conf_temp = gConfigs.get

        # self.conf = confObj.get_conf_api_dict()
        # self.totalConf = confObj
        pass

    def getApiCasId(self,email):
        urlbase = gConfigs.get("api",'API_CAS_ID').rstrip("/")
        url = urlbase + "/" + email
        r = requests.get(url)
        if not r.ok:
            r.raise_for_status()
            sys.exit()
        result = json.loads(r.content.decode())
        return(result)

    def getApiCasIdFtpGWHdir(self,email):
        apidata = self.getApiCasId(email)
        cas_user_id = apidata['user_cas_id'].strip()
        logging.info(cas_user_id)
        d1 = cas_user_id[0]
        d2 = cas_user_id[1]
        ftpdir = os.path.join(gvar.RuntimeConfig.getFtpDir(),d1,d2,cas_user_id,"GWH")
        return(ftpdir)

    def getBioProjsByBigdAccount(self,email):
        base_url =gConfigs.get("api","API_GSA_BASE_URL_GETPRJ_BY_EMAIL").rstrip("/") 
        url = base_url + "/" + email
        logger.info("getBioProjsByBigdAccount "+url)
        r = requests.get(url)
        if not r.ok:
            r.raise_for_status()
            sys.exit()
        # print(r.content.decode())
        result = json.loads(r.content.decode())
        return(result)

    def getBioSamplesByPrjAccn(self,prjaccn):
        base_url = gConfigs.get("api","API_GSA_BASE_URL_GETSAMPLE_BY_PRJACCN").rstrip("/") 
        url = base_url + "/" + prjaccn
        logging.info("Get Biosample by project ID: "+str(url))
        r = requests.get(url)
        if not r.ok:
            r.raise_for_status()
            sys.exit()
        result = json.loads(r.content.decode())
        return(result)        

    def getSampleDetails(self,SAMC):
        SAMC = SAMC.strip('"')
        base_url = gConfigs.get('api',"API_GSA_BASE_URL_GETSAMPLE_DETAIL").rstrip("/") 
        url = base_url + "/" + SAMC
        
        logging.info("Get sample detail: "+str(url))
        r = requests.get(url)
        if not r.ok:
            r.raise_for_status()
            sys.exit()
        # print(type(r.content.decode()))
        result = json.loads(r.content.decode())
        return(result)
    
    def getMaxIDs(self):
        # base_url = self.totalConf.get_value("MAXID").rstrip("/") 
        # url = base_url 
        # print(url)
        # r = requests.get(url)
        # if not r.ok:
        #     r.raise_for_status()
        #     sys.exit()
        # result = json.loads(r.content.decode())
        # pass
        sqlutil = Mysql.mysqlUtils()
        result = sqlutil.select_max_ID_from_databasemeta()
        logger.info("getmaxIDS "+ str(result))


        return(result)      

    # def getUserID(self):
        
    #     resBigdEmail = {}
    #     for r in self.contactSheetobj:
    #         bigdaccn = r[15].strip()
    #         if bigdaccn in resBigdEmail.keys():
    #             resBigdEmail[bigdaccn] += 1
    #         else: 
    #             if bigdaccn.strip() != "":
    #                 resBigdEmail[bigdaccn] = 1

    #     if len(resBigdEmail.keys()) > 1:
    #         raise Exception("the bigd account does not unique {}".format(",".join(list(resBigdEmail.keys())))) 
        
    #     self.tmp_email = list(resBigdEmail.keys())
    #     self.ftpdir = self.classApis.getApiCasIdFtpGWHdir(self.tmp_email[0])

    def getSampleInfo(self,ID):
        dat = self.getSampleDetails(ID)
        out= {}
        # type_dict = {
        #     "2":"Bacteria",
        #     "10239":"Viruses",
        #     "2157":"Archaea",
        #     "408169":"metagenomes",
        #     "4751":"Fungi",
        #     "33208":"Animals",
        #     "33090":"Plants",
        #     "2759":"Others"
        # }
        type_dict = OrderedDict()
        type_dict["2"]="Bacteria"
        type_dict["10239"]="Viruses"
        type_dict["2157"]="Archaea"
        type_dict["408169"]="metagenomes"
        type_dict["4751"]="Fungi"
        type_dict["33208"]="Animals"
        type_dict["33090"]="Plants"
        #type_dict["2759"]="Others"

        out["TaxonID"] = dat['taxon']['taxonId']
        out['organism'] = dat['taxon']['name']
        out['genebankname'] = dat['taxon']['genBankCommonName']

        if len( dat['taxon']['commonNames']) >0:

            out['commNames'] = ",".join(dat['taxon']['commonNames'])
        else:
            out['commNames'] = "NULL"

        if len(dat['taxon']['synonymNames'])>0 :
            out['synonymNames'] = ",".join(dat['taxon']['synonymNames'])
        else:
            out['synonymNames'] = "NULL"
            
        ancestors_list = dat['taxon']['ancestors']
        for k in ancestors_list:
            if str(k['taxonId'])  in type_dict.keys():
                out['type'] = type_dict[str(k['taxonId'])]
                break

        if 'type' not in out.keys():
            if str(k['taxonId'])  == "2759":
                out['type'] = "Others"
            else:
                raise Exception("TaxonID {} is not any of valid types {}".format(str(k['taxonId']),type_dict))

        out['samplename'] = dat['title']
        for k,v in out.items():
            if v == None:
                out[k] = "NULL"
        return(out)


if __name__=="__main__":
    pass


#!/usr/bin/env python
from enum import auto
import logging
# import exportExcel2Table
import json 
from core.workerGlobal import gConfigs
from core.workerGlobal import gMailManager as automail
from core.workerGlobal import gLog2File as logger
import core.eachJobGlobalVars  as gvar
from core.workerGlobal import StartAndEnd
from core.parseAPI import API
apis = API()


def validate_bioproject_biosmaples():
    '''
    校验bioproject 和biosamples
    在main.py中已经事先校验过accn是唯一的，所以这里直接get
    '''
    logger.info("validating bioprojects and biosamples via api..")

    uniqBigdAccn = gvar.bigdAccn.getBigdAccn()

    logger.info("casid email :{}".format(uniqBigdAccn))

    casid_bioprojs = apis.getBioProjsByBigdAccount(uniqBigdAccn)

    bioprj_list = []
    for ter in casid_bioprojs:
        bioprj_list.append(ter["accession"].strip())
    
    logger.info("Casid {}boprojects :\n{}".format(uniqBigdAccn, json.dumps(casid_bioprojs)))
    logger.info("Bioprojects in this accn {}: \n{}".format(uniqBigdAccn,json.dumps(bioprj_list)))

    flag = True
    bioproj_in_this_submissoin = {} # 存储这次submission的bioproject编号
    msg_bioproj = []
    msg_biosample  = []

    for assembly in gvar.excelobj.assembly:
        bioproj = assembly['Biosproject Accession'].strip("'")
        bioproj_in_this_submissoin[bioproj] = []
        if bioproj not in bioprj_list:
            flag= False
            msg_bioproj.append("The BioProject {} does not present in account {} ! ".format(bioproj,uniqBigdAccn))
    
    logger.info("Bioprojects in this submisson: \n{}".format(json.dumps(list(bioproj_in_this_submissoin.keys()))))

    for bioprjkey in bioproj_in_this_submissoin.keys():
        biosamples_in_this_submission = apis.getBioSamplesByPrjAccn(bioprjkey)
        for biosample in biosamples_in_this_submission:
            bioproj_in_this_submissoin[bioprjkey].append(biosample['accession'])

    logger.info("Biosamples in the bioproject in this submisson: \n{}".format(json.dumps(bioproj_in_this_submissoin)))

    for assembly in gvar.excelobj.assembly:
        bioproj = assembly['Biosproject Accession'].strip("'")
        biosamples  = assembly['Biosample Accession'].strip("'")

        if biosamples not in bioproj_in_this_submissoin[bioproj]:
            flag= False
            msg_biosample.append("The BioSample {} does not present in BioProject {} in this account {} !".format(biosamples,bioproj,uniqBigdAccn))
    if(len(set(msg_bioproj)) >0) or (len(set(msg_biosample))>0):
        logger.error("\n".join(msg_bioproj))
        logger.error("\n".join(msg_biosample))
        automail.mail2user("Errors detected during your submission:\n"+"\n".join(msg_bioproj) +"\n"+"\n".join(msg_biosample))
        StartAndEnd.end()
    return((flag,set(msg_bioproj),set(msg_biosample)))


def validate_wether_fa_file_is_unique():
    '''
    检查assembly中的fa是否是重复的，如果是，则报错。
    
    '''
    flag = True
    unique_fa = []
    msg = []
    for assembly in gvar.excelobj.assembly:
        
        ass_fa = str(assembly['Genome sequence file name']).strip("'")
        if ass_fa not in unique_fa:
            unique_fa.append(ass_fa)
            continue
        else:
            flag= False
            msg.append("Found duplicate Genome sequence file in Assembly sheet:\n{}".format(ass_fa))
    
    if len(msg) >0:
        logger.error(msg)
        automail.mail2user(msg)
        StartAndEnd.end()  

    return(flag)




def validate_unique_bigd_accn_and_set_globalvar():
    logger.info("validating whether bigd account is unique")
    resBigdEmail = {}
    # print(gvar.excelobj.contact)
    try:
        for r in gvar.excelobj.contact:
            bigdaccn = r["BIGD_account"].strip()
            if bigdaccn in resBigdEmail.keys():
                resBigdEmail[bigdaccn] += 1
            else: 
                if bigdaccn.strip() != "":
                    resBigdEmail[bigdaccn] = 1
    except:
        logger.error("System can not read bigd account information in Contact sheet, Please check it")
        automail.mail2user("the bigd account in Contact sheet MUST unique:\n {}")

    if len(resBigdEmail.keys()) > 1:
        logger.error("The bigd account in Contact sheet MUST unique:\n {}".format(",".join(list(resBigdEmail.keys()))))
        automail.mail2user("the bigd account in Contact sheet MUST unique:\n {}".format(",".join(list(resBigdEmail.keys()))))
        StartAndEnd.end()
        
    else:
        gvar.bigdAccn.setBigdAccn(list(resBigdEmail.keys())[0].strip("'"))
        logger.info("The bigd account in Contact sheet is unique [{}]".format(list(resBigdEmail.keys())[0]))
        return(True)





if __name__=="__main__":
    pass
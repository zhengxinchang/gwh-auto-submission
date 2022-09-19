#!/usr/bin/env python3
'''

'''

import os
import datetime
from core.workerGlobal import gConfigs
from core.workerGlobal import gMailManager as automail
from core.workerGlobal import gLog2File as logger
import core.eachJobGlobalVars  as gvar
from core.workerGlobal import StartAndEnd
from core.parseAPI import API
apis = API()

# def getBatchIDWithTimeStamp():
#     dt = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d_%H%M%S")
#     return("Batch"+dt)


def getBatchGWHAndWGSDict():
    
    assemblyobjList = gvar.excelobj.assembly
    res = apis.getMaxIDs()
    currentGWHID = res['max_accession_id']
    currentWGSID = res['max_submission_mission_id']
    logger.info("Current GWH MAX ID:{}\t Current WGS MAX ID:{}".format(currentGWHID,currentWGSID))

    WGSNumber = int(currentWGSID.strip().lstrip("WGS"))
    numOfAssembly = len(assemblyobjList)
    beforeGWHID = currentGWHID[3:7]
    afterGWHID = None

    # Genome sequence file name
    list_ID = []
    singledict = gvar.fileobject.getIsSingleDict()
    for i in range(numOfAssembly):
        out = {   
        }
        currentAssembly = assemblyobjList[i]
        sampledict = apis.getSampleInfo(currentAssembly['Biosample Accession'].strip("'"))
        afterGWHID = letter_increase(beforeGWHID)
        file_is_single = singledict.get(currentAssembly['Genome sequence file name'].strip("'"))
        logger.info("GWH ID generation, is_single in file {} is {}".format(currentAssembly['Genome sequence file name'].strip("'"),file_is_single))

        out['is_single'] = file_is_single
        if file_is_single:
            out['GWH'] = "GWH" + afterGWHID + "01000000"
        else:
            out['GWH'] = "GWH" + afterGWHID + "00000000"
        beforeGWHID = afterGWHID
        WGSNumber +=1
        
        #保证6位WGSID
        if len(str(WGSNumber)) <6:
            max_wgs = "WGS"+ "0"*(6-len(str(WGSNumber))) + str(WGSNumber)
        else:
            max_wgs = "WGS"+str(WGSNumber)
        
        out['WGS'] = max_wgs
        out['fa'] = currentAssembly['Genome sequence file name'].strip("'")
        out['gff'] = currentAssembly['Genome annotation file name'].strip("'")
        out['c2sAgp'] = currentAssembly['Contig to scaffold AGP file name'].strip("'")
        out['otherAgp'] = currentAssembly['Other AGP file name'].strip("'")
        out['chromAssig'] = currentAssembly['Chromosome assignment file name'].strip("'")
        out['plasmidfile'] = currentAssembly['Plasmid assignment file name'].strip("'")
        out['organellafile'] = currentAssembly['Organella assignment file name'].strip("'")
        out['biosample'] =  currentAssembly['Biosample Accession'].strip("'")
        out['bioproject'] =  currentAssembly['Biosproject Accession'].strip("'")
        out['assemblylevel'] = 5 - int(currentAssembly['Assembly level'])
        out['taxid'] = sampledict['TaxonID']
        out['taxonname'] =sampledict['organism']
        out['genebankname'] = sampledict['genebankname']
        out['commNames'] = sampledict['commNames']
        out['synonymNames'] = sampledict['synonymNames']
        out['samplename'] = sampledict['samplename']
        list_ID.append(out)

    # if not is_single:
    #     myout = {
    #         "metas":{"maxWGS":max_wgs,"maxGWH":"GWH" + afterGWHID + "00000000"},
    #         "datas":list_ID
    #     }    
    # else:
    #     myout = {
    #         "metas":{"maxWGS":max_wgs,"maxGWH":"GWH" + afterGWHID + "00000000"},
    #         "datas":list_ID
    #     }    
        # 重写这里，将列表最后一个加到max中
    myout = {
            "metas":{
                "maxWGS":max_wgs,
                "maxGWH":list_ID[-1]['GWH']},
            "datas":list_ID
        }   
    gvar.IDlist.setIDlist(myout) 
    return(myout)


def letter_increase(letter: str):                                                                                  
    """字母自增:A-Z-AA-AZ-BA-BZ...."""                                                                                 
    letter = letter.upper()                                                                                        
    if letter[-1] != "Z":                                                                                          
        return letter[:-1] + chr(ord(letter[-1]) + 1)                                                              
                                                                                                                   
    all_z = True                                                                                                   
    for item in letter:                                                                                            
        if item != "Z":                                                                                            
            all_z = False                                                                                          
            break                                                                                                  
    if all_z:                                                                                                      
        return "A" * (len(letter) + 1)                                                                             
    else:                                                                                                          
        start_change_index = None                                                                                  
        new_letter = None                                                                                          
        for index, item in enumerate(letter[::-1]):                                                                
            left_index = len(letter) - 1 - index    # 正序索引                                                         
            if item != "Z":                                                                                        
                new_letter = letter[:left_index] + chr(ord(letter[left_index]) + 1) + letter[left_index+1:]        
                start_change_index = left_index                                                                    
                break                                                                                              
                                                                                                                   
        new_letter = new_letter[:start_change_index + 1] + "A" * (len(letter) - start_change_index - 1)            
        return new_letter 

if __name__ == "__main__":
    pass
    # conf  = parseConfig.configs("./Config.txt")

    # excel_obj = exportExcel2Table.Excel2Objects("/mnt/e/projects/NGDC/GWH/autoBatchSubmissionPipeline/excel/GWH-batchsubmission-Chinese-small.xlsx",conf)

    # print(getBatchGWHAndWGSDict(assemblyobjList=excel_obj.getAssemblyObjsList(),conf=conf,is_single=True))

#!/usr/bin/env python3
'''
1. 读取excel中Assembly sheet中的文件名称和对应的md5
2. 扫描GWH文件夹拷贝文件到workspace,异常则报错
3. 计算文件的md5，与assembly中的进行比对,异常则报错
'''


import os
import sys
import logging
from collections import OrderedDict
import subprocess
import zipfile
import shutil
import re
import time
import gzip
import core.eachJobGlobalVars as gvar
        
from core.parseAPI import API
from core.workerGlobal import gConfigs
from core.workerGlobal import gLog2File as logger
from core.workerGlobal import gMailManager as automail
from core.workerGlobal import StartAndEnd



class dsub():
    def __init__(self):
        pass

    def get_dsub_header(self):
        cmd="#!/bin/sh\n#PBS -q {} \n#PBS -N {}\n#PBS -e {}\n#PBS -o {}\n#PBS -l mem={},walltime={},nodes=1:ppn={}\n#HSCHED -s {}".format(
            gConfigs.get("dsub",'DSUB_QUEUE'),
            gvar.RuntimeConfig.getBatchID(),
            os.path.join(gvar.RuntimeConfig.getgwhFileProcessBasedir(),gvar.RuntimeConfig.getBatchID()+".error"),
            os.path.join(gvar.RuntimeConfig.getgwhFileProcessBasedir(),gvar.RuntimeConfig.getBatchID()+".log"),
             gConfigs.get("dsub",'DSUB_MEM'),
             gConfigs.get("dsub",'DSUB_WALLTIME'),
             gConfigs.get("dsub",'DSUB_PPN'),
             gConfigs.get("dsub",'DSUB_SCHEDD')
        )
        return(cmd)

    def get_dsub_Check_header(self):
        cmd="#!/bin/sh\n#PBS -q {} \n#PBS -N {}\n#PBS -e {}\n#PBS -o {}\n#PBS -l mem={},walltime={},nodes=1:ppn={}\n#HSCHED -s {}".format(
             gConfigs.get("dsub",'DSUB_QUEUE'),
            gvar.RuntimeConfig.getBatchID(),
            os.path.join(gvar.RuntimeConfig.getgwhFileProcessBasedir(),gvar.RuntimeConfig.getBatchID(),"check",gvar.RuntimeConfig.getBatchID()+"check.error"),
            os.path.join(gvar.RuntimeConfig.getgwhFileProcessBasedir(),gvar.RuntimeConfig.getBatchID(),"check",gvar.RuntimeConfig.getBatchID()+"check.log"),
             gConfigs.get("dsub",'DSUB_MEM'),
             gConfigs.get("dsub",'DSUB_WALLTIME'),
             gConfigs.get("dsub",'DSUB_PPN'),
             gConfigs.get("dsub",'DSUB_SCHEDD')
             )
        return(cmd)
        
    def get_dsub_Stat_header(self):
        cmd="#!/bin/sh\n#PBS -q {} \n#PBS -N {}\n#PBS -e {}\n#PBS -o {}\n#PBS -l mem={},walltime={},nodes=1:ppn={}\n#HSCHED -s {}".format(
             gConfigs.get("dsub",'DSUB_QUEUE'),
            gvar.RuntimeConfig.getBatchID(),
            os.path.join(gvar.RuntimeConfig.getgwhFileProcessBasedir(),gvar.RuntimeConfig.getBatchID(),"stats",gvar.RuntimeConfig.getBatchID()+"stats.error"),
            os.path.join(gvar.RuntimeConfig.getgwhFileProcessBasedir(),gvar.RuntimeConfig.getBatchID(),"stats",gvar.RuntimeConfig.getBatchID()+"stats.log"),
             gConfigs.get("dsub",'DSUB_MEM'),
             gConfigs.get("dsub",'DSUB_WALLTIME'),
             gConfigs.get("dsub",'DSUB_PPN'),
             gConfigs.get("dsub",'DSUB_SCHEDD')
        )
        return(cmd)
    
        
    def get_dsub_shell(self,cmd):

        header = self.get_dsub_Check_header()
        total = header + "\n" + cmd
        return(total)



class fileMangager():
    
    def __init__(self):

        self.assemblySheetobj =gvar.excelobj.assembly
        self.contactSheetobj = gvar.excelobj.contact
        # self.confobj = confobj
        self.batchID = gvar.RuntimeConfig.getBatchID()
        self.classApis = API()


        workdir = gvar.RuntimeConfig.getgwhFileProcessBasedir()
        self.batchdir = os.path.join(workdir,self.batchID)
        self.batchdirSubmit = os.path.join(self.batchdir,"submit")
        self.batchdirCheck =  os.path.join(self.batchdir,"check")

        logger.info("Batch dir: {}".format(self.batchdir))

        # process contact sheet and find bigd account 
        # resBigdEmail = {}
        # for r in self.contactSheetobj:
        #     bigdaccn = r[15].strip()
        #     if bigdaccn in resBigdEmail.keys():
        #         resBigdEmail[bigdaccn] += 1
        #     else: 
        #         if bigdaccn.strip() != "":
        #             resBigdEmail[bigdaccn] = 1

        # 在main函数中已经校验         
        # if len(resBigdEmail.keys()) > 1:
        #     raise Exception("the bigd account does not unique {}".format(",".join(list(resBigdEmail.keys())))) 
        
        # self.tmp_email = list(resBigdEmail.keys())

        self.workFileList = None
        # print(ftpdir)
        if not gvar.RuntimeConfig.get_ftp_from_cmd():
            self.ftpdir = self.classApis.getApiCasIdFtpGWHdir(gvar.bigdAccn.getBigdAccn())
        else:
            self.ftpdir =  gvar.RuntimeConfig.getFtpDir()

        # if gvar.RuntimeConfig.get_ftp_from_cmd():
        #     self.ftpdir = self.ftpdir_raw
        # else:
        #     self.ftpdir = os.path.join(self.ftpdir_raw,"GWH")

        logger.info("ftpdir is: {}".format(self.ftpdir))
        
        # 通过scanftp_file 函数初始化
        self.validftpFileDict = None 

        self.scriptGen = dsub()


    def copyftp2workspace(self):
        # copy files to worksapce        
        try:
            if os.path.exists(self.batchdir):
                logger.info("Batch directory {} already exists! creating submit dir".format(self.batchdir))
                # shutil.rmtree(self.batchdir)
                # os.mkdir(self.batchdir)
                if not os.path.exists(self.batchdirSubmit):
                    os.mkdir(self.batchdirSubmit)
            else:
                logger.error("Batch directory {} not exists! please create first".format(self.batchdir))
                # os.mkdir(self.batchdir)
                # os.mkdir(self.batchdirSubmit)
                # automail.mail2manager("Batch directory {} not exists! please create first".format(self.batchdir))
                StartAndEnd.end()
                

            self.workFileList = []
            errorlogs = []
            for k,v in self.validftpFileDict.items():
                    try:                
                        logger.info("try to copy file {} to workspace".format(v))
                        shutil.copyfile(v,os.path.join(self.batchdirSubmit,k))
                        self.workFileList.append(os.path.join(self.batchdirSubmit,k))
                    except Exception as e :
                        errorlogs.append("Error when copy file {} to {} reasons:{}".format(v,os.path.join(self.batchdirSubmit,k),e))
            if len(errorlogs) ==0:
                logger.info("copy files from ftpdir to workdir successfully.")
                
                return([True,None])
            else:
                automail.mail2user("\n".join(errorlogs))
                automail.mail2manager()("\n".join(errorlogs))
                StartAndEnd.end()
                
        except Exception as x:
            logger.error("INNER_ERROR: Error occured during copy files from ftpdir to workdir:\n" + str(x))
            # automail.mail2manager("INNER_ERROR: Error occured during copy files from ftpdir to workdir:\n" + str(x))
            StartAndEnd.end()

       

    def getFastaIsSingle(self): # 修改为给IDlist追加isSingle的信息

        is_single_info = {}

        errorlog = []

        if self.workFileList == None:
            logger.error("workFileList not initialized, it's seems copyftp2workspace not working correctly...")
            # automail.mail2manager("workFileList not initialized, it's seems copyftp2workspace not working correctly...")
            StartAndEnd.end()
        for k in self.workFileList:
            try:
                bname = os.path.basename(k)
                if bname.upper().endswith("FA") or bname.upper().endswith("FASTA"):
                    count = 0
                    is_single_info[bname] = True
                    with open(k) as f:
                        for r in f:
                            if r.strip().startswith(">"):
                                count += 1
                            if count >1:
                                is_single_info[bname]=False
                                break 
                        if count ==0:#这里需要判断是否是0个>如果是，则报错
                            logger.error("Can not detect > in file {}, maybe not in fasta format".format(k))
                            errorlog.append("Can not detect > in file {}, maybe not in fasta format".format(k))
                         
                elif bname.upper().endswith("GZ"):
                    count = 0
                    is_single_info[bname] = True
                    with gzip.open(k, "rt") as file:
                        for r in file:
                            # print(r)
                            if r.strip().startswith(">"):
                                count += 1
                            if count >1:
                                is_single_info[bname]=False
                                break 
                        if count ==0:#这里需要判断是否是0个>如果是，则报错
                            logger.error("Can not detect > in file {}, maybe not in fasta format".format(k))
                            errorlog.append("Can not detect > in file {}, maybe not in fasta format".format(k))
                # else:
                #     errorlog.append("Errors when get is_single information: File name {} must end with fa or fasta or fa.gz or fasta.gz".format(bname))

            except Exception as e :
                errorlog.append("Errors when get is_single information: "+e)

        if len(errorlog) >0:
            logger.info("Found errors when getFastaIsSingle()")
            automail.mail2user("Found errors when getFastaIsSingle():\n{}".format("\n".join(errorlog)))  
            StartAndEnd.end()
        else:  
            logger.info("getFastaIsSingle() finished, updating to gvars")

            gvar.fileobject.setIsSingleDict(is_single_info)
        # return(is_single_info)

    def calculateMD5(self):

    
        logger.info("Calculating MD5")
        fileInOne = " ".join(self.workFileList)
        cmd = "md5sum " + fileInOne  # + "> "  + os.path.join(self.batchdirSubmit,"md5sum.txt")  

        res = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out=res.stdout.readlines()
        md5calres = {}
        for a in out:
            md5,longfile = re.split(" +",a.decode().strip())
            shortf = os.path.split(longfile)[1]
            md5calres[shortf] = md5
        fileAndMd5 = self.__getFileListAndMd5()

        md5ok = True
        badmd5file = []
        for k,v in fileAndMd5.items():
            if str(v).upper() != str(md5calres[k]).upper():
                md5ok =False
                badmd5file.append(" ".join([k,"MD5(Excel):",v,"MD5(file):",md5calres[k]]))
        
        if md5ok == False:
            logger.error("Found different MD5 between Excel and File :\n{}".format("\n".join(badmd5file)))
            automail.mail2user("Found different MD5 between Excel and File :\n{}".format("\n".join(badmd5file)))
            StartAndEnd.end()
        else:
            logger.error("check MD5 ok")
        # return({"md5isok":md5ok,"badmd5list":badmd5file}) 

    def runQCSehll(self):
        IDlist = gvar.IDlist.getIDlist()
        try:
            if not os.path.exists(self.batchdirCheck):
                os.mkdir(self.batchdirCheck)
            else:
                shutil.rmtree(self.batchdirCheck)
                os.mkdir(self.batchdirCheck)
            shells = self.__genQCshell(IDlist['datas'])
            # print(shells)
            with open(os.path.join(self.batchdirCheck,self.batchID+"_check.sh"),"w") as f :
                f.write(shells)
            subprocess.Popen("dsub  " + os.path.join(self.batchdirCheck,self.batchID+"_check.sh"),shell=True)
        except Exception as e:
            logger.error("error when dsub shell reason:{}".format(e))
            # automail.mail2manager("error when dsub shell reason:{}".format(e))
            StartAndEnd.end()

        else:
            return([True,None])


    def checkError(self,debug=False):

        if debug:
            pass
            # return([False,""])

        logger.info("Waiting Job Done...")
        while True:
            time.sleep(5)
            # logger.info("Waiting Job Done...")
            if os.path.exists(os.path.join(self.batchdirCheck,self.batchID+"check.error")) and os.path.exists(os.path.join(self.batchdirCheck,self.batchID+"check.log")):
                break
        logger.info("Job Done...")
        
        errlist = []
        for root,dirs,files in os.walk(self.batchdirCheck):
            for f in files:
                if f.endswith(".err") :
                    if not f.startswith("Batch"):
                        errlist.append(os.path.join(root,f))
        
        wether_has_invalid_error = False
        has_err = []
        for err in errlist:
            if os.path.getsize(err) != 0:
                has_err.append(err)
                wether_has_invalid_error  = True 

        if wether_has_invalid_error:
            zp=zipfile.ZipFile(os.path.join(self.batchdirCheck,self.batchID + "_Invalid_errs.zip"),'w', zipfile.ZIP_DEFLATED)
            
            for file in has_err:
                zp.write(file,os.path.split(file)[1])
            zp.close()
            time.sleep(5)
        
            # print([wether_has_invalid_error,])
            logger.error("Found QC errors")
            automail.mail2user("Found QC errors, please see detail in attachment file",file=os.path.join(self.batchdirCheck,self.batchID + "_Invalid_errs.zip"))
            StartAndEnd.end()




        

    def __genQCshell(self,IDlist):
        
        """
        cd /p300/gwhfileprocess/Batch000070/ # {}
        /usr/bin/perl # config
        /p300/gwhsoftware/Program/QCprogram20170518/CheckFilesCtg2xv1.pl  #config
        -name NH090.fa  #{}
        -taxid 4577  #{}
        -Nlen  10   # config
        -assemblylevel 1 #{}
        -q /p300/gwhfileprocess/Batch000070/submit20210127/NH090.fa  #{} 
        -annotation /p300/gwhfileprocess/Batch000070/submit20210127/NH090.gff3 #{} 
        -o /p300/gwhfileprocess/Batch000070/check20210127/ #{}
        """



        convert = {
        "1":"4",
        "2":"3",
        "3":"2",
        "4":"1"
        }

        logging.info("start gen shell")

        cmds = []
        cmds.append("cd " + self.batchdirCheck)
        #print(self.assemblySheetobj)
        # print(IDlist)
        for r in self.assemblySheetobj:
            
            taxid = None
            for k in IDlist:
                # print(k)
                if r["Biosample Accession"].strip().strip("'") == k['biosample']:
                    taxid =  str(k['taxid'])
            if taxid == None :

                logger.error("can not found correct taxid for biosample {}".format(r["Biosample Accession"].strip().strip("'")))
                automail.mail2manager("can not found correct taxid for biosample {}".format(r["Biosample Accession"].strip().strip("'")))
                automail.mail2user("can not found correct taxid for biosample {}".format(r["Biosample Accession"].strip().strip("'")))
                StartAndEnd.end()

            params = {}
            if r["Genome sequence file name"] != "":
                params["-name"] = r["Genome sequence file name"].strip()
                params["-q"] = os.path.join(self.batchdirSubmit,r["Genome sequence file name"].strip())
            if r["Assembly level"] != "":
                params["-assemblylevel"] = convert[str(r["Assembly level"])]
            if r["Genome annotation file name"] != "":
                params["-annotation"] = os.path.join(self.batchdirSubmit,r["Genome annotation file name"].strip())
            if r["Organella assignment file name"] !="":
                params["-csvorganella"] =   os.path.join(self.batchdirSubmit,r["Organella assignment file name"].strip())
            if r["Chromosome assignment file name"] !="":
                params["-csvchr"] =   os.path.join(self.batchdirSubmit,r["Chromosome assignment file name"].strip())
            if r["Plasmid assignment file name"] !="":
                params["-csvplasmid"] =   os.path.join(self.batchdirSubmit,r["Plasmid assignment file name"].strip())

            params["-taxid"] = taxid
            params['-Nlen'] = gConfigs.get("qc","Nlen")
            params['-o'] =  self.batchdirCheck
            # print(params)
            onecmd = "  ".join([
                    gConfigs.get("qc","PERL"),
                    gConfigs.get("qc","CheckFilesCtg2xv1"),
                    "-name",params['-name'],
                    "-Nlen",params['-Nlen'],
                    "-taxid",params['-taxid'],
                    "-assemblylevel" if "-assemblylevel" in params.keys() else "" ,
                    params['-assemblylevel']  if "-assemblylevel" in params.keys() else "" ,
                    "-q",params['-q'],
                    "-annotation"  if "-annotation" in params.keys() else "",
                    params['-annotation']  if "-annotation" in params.keys() else "",
                    "-csvorganella"  if "-csvorganella" in params.keys() else "",
                    params['-csvorganella']  if "-csvorganella" in params.keys() else "",
                    "-csvchr"  if "-csvchr" in params.keys() else "",
                    params['-csvchr']  if "-csvchr" in params.keys() else "",
                    "-csvplasmid"  if "-csvplasmid" in params.keys() else "",
                    params['-csvplasmid']  if "-csvplasmid" in params.keys() else "",
                    "-o",params["-o"]
                ])

            # print(onecmd)
            if r["Sequence Contamination QC"].strip().upper() == "Y" or r["Sequence Contamination QC"].strip().upper() == "YES":
                onecmd = onecmd + "\n " + "  ".join([
                    gConfigs.get("qc","PERL"),
                    gConfigs.get("qc","QC_Contamination"),
                    "-name",params['-name'],
                    "-t /p300/gwhsoftware/Program/QCprogram20170518/UniVec_db/UniVec",
                    "-q",params['-q'],
                    "-o",params["-o"]
                ]) + "\n"
            # print(onecmd)
            cmds.append(onecmd)
        
        # // import from dsubShell Manager
        shell = self.scriptGen.get_dsub_shell(cmd = "\n".join(cmds))
        logger.info("shell is ".format(shell)) 
        return(shell)

    def scanftp_file(self):
        "scan ftp.../GWH dir and compare it to file list"
        errorlog = []
        fileAndMd5 = self.__getFileListAndMd5()
        if not os.path.exists(self.ftpdir):
            logger.error("CASID path {} does not exists...".format(self.ftpdir))
            StartAndEnd.end()
        
        # 循环遍历所有文件,允许有重复文件
        filedict = {}
        for root,dirs,files in os.walk(self.ftpdir):
            for name in files:
                filedict[os.path.join(root,name)] = name
        logger.info("files in ftp dir:\n{}".format(str(filedict)))
        # 检查execl中的文件是否存在ftp存在文件名称重复
        dupdict = {}
        for k,v  in filedict.items():
            if v in dupdict.keys():
                dupdict[v] += 1
            else:
                dupdict[v] = 1
        for k,v in dupdict.items():
            if v >1 and k in fileAndMd5.keys():
                errorlog.append("File {} in Assembly sheet have duplicate copy(maybe with diff path) in ftp".format(k))

        # 检查是否excel中的文件在ftp中不存在(已经保证所有的文件名都不重复)
        # print("fileAndMd5: "+str(fileAndMd5.keys()))
        # print("fileinftp: "+str(filedict.values()))
        for f in fileAndMd5.keys():
            # print(f)
            if f not in filedict.values():
                errorlog.append("Found {} in Assembly Sheet but not detected in ftp dir!".format(f))

        out = {}
        for k,v in filedict.items():
            if v in fileAndMd5.keys():
                out[v] =  k 
        self.validftpFileDict = out
        
        
        if len(errorlog) == 0:

            return([True,""])
        else:

            logger.error("Scan ftp dir fail...")
            automail.mail2user("errors occured when scan files in ftp:\n"+"\n".join(errorlog))
            StartAndEnd.end()
            
    
    def __getFileListAndMd5(self):
        
        # 全部修改为keyvalue
        out = OrderedDict()
        errorlog = []
        # process fa
        for r in self.assemblySheetobj:
            fn = r["Genome sequence file name"].strip().strip("'")
            md5 = r["Genome sequence file MD5 code"].strip().strip("'")
            # print(fn)
            if fn != "" and fn !="NULL":
                # assume filename and md5 not dupolicate
                if fn in out.keys() or md5 in out.values():
                    
                    errorlog.append("Genome sequence file name  Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    # logger.error("Genome sequence file name  Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    # automail.mail2user("Genome sequence file  name Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    # StartAndEnd.end()
                 
                else:
                    out[fn] = md5

        # process gff3
        for r in self.assemblySheetobj:
            fn = r["Genome annotation file name"].strip().strip("'")
            md5 = r["Genome annotation file MD5 code"].strip().strip("'")
            # print(fn)
            if fn != "" and fn !="NULL":
            # assume filename and md5 not dupolicate
                # print("inner"+fn)
                if fn in out.keys() or md5 in out.values():
                    
                    errorlog.append("Genome annotation file name Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    # logger.error("Genome annotation file name Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    # automail.mail2user("Genome annotation file name Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    # StartAndEnd.end()
                else:
                    out[fn] = md5

        # 以下不需要强制每一行都是不同文件
        # process agp
        for r in self.assemblySheetobj:
            fn = r["Contig to scaffold AGP file name"].strip().strip("'")
            md5 = r["Contig to scaffold AGP file MD5 code"].strip().strip("'")

            if fn != "" and fn !="NULL":
            # assume filename and md5 not dupolicate
                if fn in out.keys() or md5 in out.values():
                    # pass
                    out[fn] = md5
                    # raise   Exception("Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                else:
                    out[fn] = md5
    
        # process other agp
        for r in self.assemblySheetobj:
            fn = r["Other AGP file name"].strip().strip("'")
            
            md5 = r["Other AGP file MD5 code"].strip().strip("'")

            if fn != "" and fn !="NULL":
            # assume filename and md5 not dupolicate
                if fn in out.keys() or md5 in out.values():
                    # pass
                    # raise   Exception("Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    out[fn] = md5
                else:
                    out[fn] = md5

        # process assignment
        for r in self.assemblySheetobj:
            fn = r["Chromosome assignment file name"].strip().strip("'")
            md5 = r["Chromosome assignment file MD5 code"].strip().strip("'")

            if fn != "" and fn !="NULL":
            # assume filename and md5 not dupolicate
                if fn in out.keys() or md5 in out.values():
                    out[fn] = md5
                    # raise   Exception("Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                else:
                    out[fn] = md5
        
        # process plasmid
        for r in self.assemblySheetobj:
            fn = r["Plasmid assignment file name"].strip().strip("'")
            md5 = r["Plasmid assignment file MD5 code"].strip().strip("'")

            if fn != "" and fn !="NULL":
            # assume filename and md5 not dupolicate
                if fn in out.keys() or md5 in out.values():
                    out[fn] = md5
                    # raise   Exception("Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                else:
                    out[fn] = md5

        #Organell
        for r in self.assemblySheetobj:
            fn = r["Organella assignment file name"].strip().strip("'")
            md5 = r["Organella assignment file MD5 code"].strip().strip("'")

            if fn != "" and fn !="NULL":
            # assume filename and md5 not dupolicate
                if fn in out.keys() or md5 in out.values():
                    # raise   Exception("Found Duplicate Filename {} or MD5 {}".format(fn,md5))
                    out[fn] = md5
                else:
                    out[fn] = md5
        logger.info("All file list in excel assembly sheet:\n" + str(out))

        if len(errorlog) >0:
            logger.error("Found errors [excel validator]:\n Genome sequence file name (and MD5) in assembly sheet must be unique:\n{}".format("\n".join(errorlog)))
            automail.mail2user("Found errors [excel validator]:\n{}".format("\n".join(errorlog)))
            StartAndEnd.end()

        return(out)


            
if __name__== "__main__":
    pass
    # automail = autoMail.send_mail()

    # e2t = exportExcel2Table.ExcelExport(confs.get_value("EXCEL"))   
    # assemly_sheet = e2t.exportAssemblySheet()
    # contact_sheet = e2t.exportContactSheet()



    # ckcp =  CheckAndCopyMangager(assemly_sheet,contact_sheet,confs,BatchID)
    
    
    # res = ckcp.copyftp2workspace()
    # logging.info(res)
    
    # logging.info(ckcp.getFastaIsSingle())
    # md5res = ckcp.calculateMD5()

    # if not md5res['md5isok']:
    #     automail.auto_mail_attach("您好，\n您的部分文件MD5码有误：\n {} \n祝好\ngwh team".format("\n".join(md5res['badmd5list'])),"GWH FeedBack(auto)",['zhengxc_auto@163.com'])

    # ckcp.runQCSehll("4577")

    # res = ckcp.checkError()
    # if res[0]:

    #     automail.auto_mail_attach("您好，\n您的部分文件未通过质控，请根据附件信息进行修改\n祝好\ngwh team","GWH FeedBack(auto)",['zhengxc_auto@163.com'],res[1])


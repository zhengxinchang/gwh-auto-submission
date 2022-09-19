#!/usr/bin/env python3

# from insertIntoGWHdb import IDlist
import os 
# import parseAPI
# import dsubShell
# import exportExcel2Table
# import batchIDGen
import logging
import shutil
import subprocess
import time 

from core.parseAPI import API
from core.workerGlobal import gConfigs
from core.workerGlobal import gLog2File as logger
from core.workerGlobal import gMailManager as automail
from core.workerGlobal import StartAndEnd
import core.eachJobGlobalVars as gvar
import core.fileManager 

'''

#!/bin/sh
#PBS -q workq
#PBS -N Batch000068_sta
#PBS -e /p300/gwhfileprocess/Batch000068/Batch000068_sta.log
#PBS -o /p300/gwhfileprocess/Batch000068/Batch000068_sta.out
#PBS -l mem=16gb,walltime=24:00:00,nodes=1:ppn=6
#HSCHED -s hschedd
cd /p300/gwhfileprocess/
mkdir WGS017987
cp /p300/gwhfileprocess/Batch000068/submit20210125/0411_4.fasta /p300/gwhfileprocess/WGS017987/
cp /p300/gwhfileprocess/Batch000068/submit20210125/0411_4.gff /p300/gwhfileprocess/WGS017987/
cd /p300/gwhfileprocess/WGS017987/
perl /p300/gwhsoftware/Program/stats/stats.pl -name GWHAZPP01000000 -assemblylevel 4 -accession GWHAZPP01000000 -taxid 2697049 -genome /p300/gwhfileprocess/WGS017987/0411_4.fasta -annotation /p300/gwhfileprocess/WGS017987/0411_4.gff -o /p300/gwhfileprocess/WGS017987/
/usr/bin/perl /p300/gwhsoftware/Program/jbrowse/bin/prepare-refseqs.pl --fasta /p300/gwhfileprocess/WGS017987/GWHAZPP01000000.genome.fasta
/usr/bin/perl /p300/gwhsoftware/Program/jbrowse/bin/flatfile-to-json.pl --gff /p300/gwhfileprocess/WGS017987/GWHAZPP01000000.gff --trackLabel "Genome_annotation"
/usr/bin/perl /p300/gwhsoftware/Program/perlprograms/jbrowsetrack.pl  -org "Severe acute respiratory syndrome coronavirus 2" -acc GWHAZPP01000000 -track data/tracks.conf  -jbconf data/jbconf_to_append.txt
mkdir -p /p300/gwhFileSite/GWHAZPP01000000
cp -r * /p300/gwhFileSite/GWHAZPP01000000/

'''

class StatShell():
    
    def __init__(self):
        self.IDlistdata = gvar.IDlist.IDlist['datas']
        self.IDlistmeta = gvar.IDlist.IDlist['metas']
        self.outIDlistdata = [] # initial copy of IDlist and this variable will be return by func. genStatShell
        self.batchID = gvar.RuntimeConfig.getBatchID()
        self.assemblyDict = gvar.excelobj.getAssembly()
        self.contactDict = gvar.excelobj.getContact()
        self.publicationDict = gvar.excelobj.getPublication()
        self.workdir = gvar.RuntimeConfig.getgwhFileProcessBasedir()
        self.filesitedir = gvar.RuntimeConfig.getgwhFileSitesBasedir()
        self.batchdir = os.path.join(self.workdir,self.batchID)
        self.batchdirSubmit = os.path.join(self.batchdir,"submit")
        self.batchdirCheck =  os.path.join(self.batchdir,"check")
        self.batchdirStat = os.path.join(self.batchdir,"stats")
        self.dsubmanager = core.fileManager.dsub()
        self.apis = API()
    def genStatShell(self):
        """生成脚本,
            返回脚本命令，
            返回记录的对应路径
        """
    
        stat_shell_header = self.dsubmanager.get_dsub_Stat_header()


        for k in self.IDlistdata:

            copyk = k #copy of one record in IDlist
            copyk['WGSdir'] =  os.path.join(self.workdir,k["WGS"])
            copyk['GWHdir'] = os.path.join(self.filesitedir,k['GWH'])
            self.outIDlistdata.append(copyk)

            wgsdir = os.path.join(self.workdir,k["WGS"])
            cplist = []
            for d in ['fa' ,'gff' ,'c2sAgp' ,'otherAgp' ,'chromAssig' ,'plasmidfile' ,'organellafile' ]:
                if k[d].strip("'") != "NULL":
                    t = "cp {} {}".format(os.path.join(self.batchdirSubmit,k[d]),wgsdir)
                    cplist.append(t)
            sampledict = self.apis.getSampleDetails(k['biosample'])

            mkwgsdir_cmd = "mkdir -p {0}  \ncd {0} \n{1}".format(wgsdir,"\n".join(cplist))
            
            stat_cmd = "{} {} -name {} -assemblylevel {} -accession {} -taxid {} -genome {} ".format(
                gConfigs.get("qc",'PERL'),
                gConfigs.get("stats","stats"),
                k["GWH"],
                k['assemblylevel'],
                k['GWH'],
                sampledict['taxon']['taxonId'],
                os.path.join(wgsdir,k['fa'])
            )
            if k['gff'] != "NULL":
                stat_cmd += " -annotation {}".format(k['gff']) 
            if k['organellafile'] != "NULL":
                stat_cmd += " -csvorganella {}".format(k['organellafile']) 
            if k['chromAssig'] != "NULL":
                stat_cmd += " -csvchr {}".format(k['chromAssig']) 
            if k['plasmidfile'] != "NULL":
                stat_cmd += " -csvplasmid {}".format(k['plasmidfile']) 
            
            stat_cmd += "  -o {}".format(wgsdir)


            jb_cmd = "\n{0}  {1} --fasta {2} ".format(
                 gConfigs.get("qc",'PERL'),
                 gConfigs.get("stats",'prepare-refseqs'),
                 os.path.join(wgsdir,k['GWH']+".genome.fa")
            )

            if k['gff'] != "NULL":
                # jb_cmd += '''\n{0}  {1} --gff /p300/gwhfileprocess/WGS017987/GWHAZPP01000000.gff --trackLabel "Genome_annotation" '''.format(
                jb_cmd += '''\n{0}  {1} --gff {2} --trackLabel "Genome_annotation" '''.format(
                 gConfigs.get("qc",'PERL'),
                 gConfigs.get("stats",'flatfile-to-json'),
                 os.path.join(wgsdir,k['GWH']+".genome.gff")
                )

            jb_cmd += '''\n{0}  {1} -org "{2}" -acc {3} -track data/tracks.conf  -jbconf data/jbconf_to_append.txt'''.format(
            gConfigs.get("qc",'PERL'),
            gConfigs.get("stats",'jbrowsetrack'),
            sampledict['taxon']['name'],
            k['GWH'],
            )
                

            cpdir_cmd = """mkdir -p {0}  \ncp -r {1} {0}""".format(
                os.path.join(self.filesitedir,k['GWH']),
                os.path.join(wgsdir,"*")
            )


            final_shell = "\n".join([mkwgsdir_cmd , stat_cmd , jb_cmd , cpdir_cmd])
            stat_shell_header += "\n" + final_shell + "\n\n"

        return([stat_shell_header,self.outIDlistdata])


    def runStatShell(self):
        """运行脚本"""
        try:
            if not os.path.exists(self.batchdirStat):
                os.mkdir(self.batchdirStat)
            else:
                shutil.rmtree(self.batchdirStat)
                os.mkdir(self.batchdirStat)
            shells = self.genStatShell()[0]
            logger.info("generate Stat shell \n{}".format(shells))
            with open(os.path.join(self.batchdirStat,self.batchID+"_stats.sh"),"w") as f :
                f.write(shells)
            res = subprocess.Popen("dsub " + os.path.join(self.batchdirStat,self.batchID+"_stats.sh"),shell=True)
        except Exception as e:
            logger.info("error when dsub shell reason:{}".format(e))
            return([False,e])
        else:
            return([True,res])



    def waitDone(self,debug=False):
    
        if debug:
            return([False,""])
        logger.info("Waiting Job Done...")
        while True:
            time.sleep(5)
            
            if os.path.exists(os.path.join(self.batchdirStat,self.batchID+"stats.error")) and os.path.exists(os.path.join(self.batchdirStat,self.batchID+"stats.log")):
                break
        logger.info("Job Done...")
    def scanStatsRes(self):
        """ 根据"""

        # 根据self.outIDlist中的wgsdir 和 GWHdir扫描对应的文件，读入并且返回列表
        # self.outIDlist = [ { WGSdir,GWHdir,。。。}, {}]

        # 每个字典中增加三个key-value
        # assembly.stats []
        # detail.stats []
        # whole.stats []
        newOutIDlist = []
        for onerecord in self.outIDlistdata:
            wgsdir = onerecord['WGSdir']
            files = os.listdir(wgsdir)
            
            newOneRrecord = onerecord
            logger.info("scanStatRes:{}".format("\t".join(files)))
            for f in files:
                if f.endswith(".assembly.stats"):
                    newOneRrecord["assembly.stats"] = {}
                    with open(os.path.join(wgsdir,f)) as f:
                        for r in f:
                            r = r.strip()
                            c = r.split("\t")
                            assert len(c) == 2 ,"assembly.stats must have two fileds" 
                            newOneRrecord["assembly.stats"][c[0]]= c[1]
                elif f.endswith(".detail.stats"):
                    newOneRrecord['detail.stats'] = {}
                    with open(os.path.join(wgsdir,f)) as f:
                        count = 0
                        for r in f:
                            count += 1
                            if count == 1:
                                continue
                            r = r.strip()
                            c = r.split("\t")
                            assert len(c) == 8 ,"detial.stats must have eight fileds" 
                            newOneRrecord["detail.stats"][c[0]]= c[1:]
                elif f.endswith(".whole.stats"):
                    newOneRrecord['whole.stats'] = {}
                    with open(os.path.join(wgsdir,f)) as f:
                        count = 0
                        for r in f:
                            count += 1
                            if count == 1:
                                continue
                            r = r.strip()
                            c = r.split("\t")
                            assert len(c) == 8 ,"whole.stats must have eight fileds" 
                            newOneRrecord["whole.stats"][c[0]]= c[1:]

            newOutIDlist.append(newOneRrecord)
        # print("addtinal IDlist")
        logger.info("IDlist with stats information: " + str({"metas":self.IDlistmeta,"datas":newOutIDlist}))
        gvar.IDlist_with_stats.setIDlist({"metas":self.IDlistmeta,"datas":newOutIDlist})
        # return({"metas":self.IDlistmeta,"datas":newOutIDlist})


if __name__ =="__main__":

    # conf = parseConfig.configs("./Config.txt")
    # excel  = exportExcel2Table.Excel2Objects(conf.get_value("EXCEL"),confobj=conf)

    # ContactObj = excel.getContactObjsList()
    # PublicatinObj = excel.getPublicationObjsList()
    # AssemblyObj = excel.getAssemblyObjsList()
    pass
    # batchID = batchIDGen.getBatchIDWithTimeStamp()
    # IDlist = batchIDGen.getBatchGWHAndWGSDict(AssemblyObj,conf) 
    # ss = StatShell(conf,batchID=batchID,assemblyDict=AssemblyObj,contactDict=ContactObj,publicationDict=PublicatinObj,IDlist=IDlist)
    # print(ss.genAndRunStatShell())
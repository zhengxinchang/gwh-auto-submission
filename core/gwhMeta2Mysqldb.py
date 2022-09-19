#!/usr/bin/env python3
"""
Mysql 是操作gwh数据库的工具类，对应13个表的插入函数，实例初始化时，游标建立，开启事务，使用 utils.close()时执行commit 完成事务。
通过export2Table中的EXCEL2Obj类获得三个表的对象，格式是列表，每个元素是字典，键为列名，值为具体值

"""

import re
import logging
import core.mysqlManager as Mysql
import datetime
from  core.parseAPI import API
import pymysql

from core.parseAPI import API
from core.workerGlobal import gConfigs
from core.workerGlobal import gLog2File as logger
from core.workerGlobal import gMailManager as automail
from core.workerGlobal import StartAndEnd
import  core.eachJobGlobalVars as gvar



class GWHdbManager():


    def __init__(self,):
        # self.configs = configobj
        self.apis = API()
        
    def updateGWHIDandWGSIDtoDB(self):
        """
        解析IDlist中的最大WGSID和GWHID，更新meta_database
        """ 

        IDlist = gvar.IDlist.IDlist

        hits = False
        IDlist_meta = IDlist['metas']
        try:
            maxWGS = IDlist_meta["maxWGS"]
            maxGWH = IDlist_meta['maxGWH']
            hits = True
        except:
            hits = False 

        if hits:
            sqlutil = Mysql.mysqlUtils()
            is_ok = sqlutil.update_max_ID_to_table_databasemeta(maxWGS,maxGWH)
            if is_ok:
                sqlutil.commit()
                sqlutil.close()
                logger.info("Update Max IDs(maxWGS,maxGWHID) successfully")
                return(True)
            else:
                sqlutil.rollback()
                sqlutil.close()
                logger.info("can not update Max IDs(maxWGS,maxGWHID)")
                # automail.mail2manager("can not update Max IDs(maxWGS,maxGWHID)")
                StartAndEnd.end()
                return(False) 
        else:
            logger.error("Can not detected maxWGS and maxGWH in IDlist:{}".format(str(IDlist)))
            # automail.mail2manager("Can not detected maxWGS and maxGWH in IDlist:{}".format(str(IDlist)))
            StartAndEnd.end()


    def insertExcelInfoRecords(self,is_control=False):
        """插入除了journal之外的表格"""
        # 事务已经开启

        ## 检测bigd account 是不是唯一的

        contactobj = gvar.excelobj.contact
        assemblyobj = gvar.excelobj.assembly
        publicationojb = gvar.excelobj.publication
        IDlist = gvar.IDlist_with_stats.newIDlist


        # resBigdEmail = {}
        # for r in contactobj:
        #     bigdaccn = r["BIGD_account"].strip().strip("'").strip()
        #     if bigdaccn in resBigdEmail.keys():
        #         resBigdEmail[bigdaccn] += 1
        #     else: 
        #         if bigdaccn.strip() != "":
        #             resBigdEmail[bigdaccn] = 1

        # if len(resBigdEmail.keys()) > 1:
        #     raise Exception("the bigd account does not unique {}".format(",".join(list(resBigdEmail.keys())))) 
        
        # print(resBigdEmail)
        # self.tmp_email = list(resBigdEmail.keys())
        self.userdict = self.apis.getApiCasId(gvar.bigdAccn.accn)
        # print(self.userdict)
        self.userid = self.userdict['user_id'].strip()

       
        is_succeed = True # 记录整个插入过程是否是成功的，最终的返回值
        # logging.info(IDlist)

        # 返回所有的 GWH WGSID 以及对应的文件列表

        # already_insertedID = {}

        assemblyobj = gvar.excelobj.assembly


        """将每条assembly的事物改为整个assembly表一个事物，要么都成功，要么都失败。"""
        # try:

        sqlutil = Mysql.mysqlUtils()
        for r_i,r_assembly in enumerate(assemblyobj):
        
            # 从整个IDlist中提取对应biosample的记录信息
            # 这里是通过biosample提取数据，所以biosample相同，则只能够插入一个。
            # 这里切换为仅有一个
            this_ID = None 
            for k in IDlist['datas']:

                # if str(r_assembly['Biosample Accession']).strip("'") == k['biosample']:
                #     this_ID = k
                if str(r_assembly['Genome sequence file name']).strip("'") == k['fa']:
                    this_ID = k
            if this_ID == None:
                
                logger.error("information for biosample {} not in ID list !!!".format(str(r_assembly['Biosample Accession']).strip("'")))
                # automail.mail2manager("information for biosample {} not in ID list !!!".format(str(r_assembly['Biosample Accession']).strip("'")))
                StartAndEnd.end()


            this_GWHID = this_ID['GWH']
            this_WGSID = this_ID['WGS']


            # 获取对应的contact和publication的数据
            related_contact= None
            for r_contact in contactobj:
                if r_contact['CID'] == r_assembly["CID"]:
                    related_contact = r_contact
            related_publication = None 
            for r_publication in publicationojb:
                if r_publication["PID"] ==  r_assembly["PID"]:
                    related_publication = r_publication

            if related_contact == None or related_publication == None:
                
                logger.error("Can not find related CID or PID for GID:{}".format(r_assembly['GID']))
                # automail.mail2manager("Can not find related CID or PID for GID:{}".format(r_assembly['GID']))
                StartAndEnd.end()
            
            

            # 开始插入数据库
        
                
                # 插入table journal
            contryID = sqlutil.select_country(related_contact["Country/Region"].strip('"'))

            # submitter table
            submitter_insert_id = sqlutil.insert_table_submitter(
                account_non_expired=True,
                account_non_locked=True,
                city= related_contact["City"],
                credentials_non_expired=True,
                department=related_contact["Department"],
                email=related_contact["Email"],
                enabled=True,
                fax=related_contact["Fax"],
                first_name=related_contact['First name'],
                last_name=related_contact['Last name'],
                middle_name=related_contact['Middle name'],
                organization=related_contact['Organization'],
                phone=related_contact['Phone'],
                postal_code=related_contact['Postal code'],
                state=related_contact['State/Province'],
                street=related_contact['Street'],
                country_id=contryID[0]
            )
            logger.info("Submitter insert ID {}".format(str(submitter_insert_id)))

            # assignment table
            assignment_insert_id = sqlutil.insert_table_assignment(
                chromosome_assigned= False if (r_assembly["Chromosome assignment file name"] =="NULL" and r_assembly["Chromosome assignment file MD5 code"] =="NULL" ) else True ,
                chromosome_filename= r_assembly["Chromosome assignment file name"], 
                chromosome_file_md5= r_assembly["Chromosome assignment file MD5 code"],
                # chromosome_upload= "b'0'" if (r_assembly["Chromosome assignment file name"] =="NULL" and r_assembly["Chromosome assignment file MD5 code"] =="NULL" ) else "b'1'" ,
                chromosome_upload= None if (r_assembly["Chromosome assignment file name"] =="NULL" and r_assembly["Chromosome assignment file MD5 code"] =="NULL" ) else True ,
                gap_length_type= None if (str(4 - int(r_assembly['Assembly level'])) == 3) else "Estimated length",
                gap_type=None if (str(4 - int(r_assembly['Assembly level'])) == 3) else "Pair-ends",
                minimum_gap_length= 0 if (str(4 - int(r_assembly['Assembly level'])) == 3) else 10,
                organella_assigned= False if (r_assembly["Organella assignment file name"] =="NULL" and r_assembly["Organella assignment file MD5 code"] =="NULL" ) else True,
                organella_filename= r_assembly['Organella assignment file name'],
                organella_file_md5= r_assembly['Organella assignment file MD5 code'],
                # organella_upload= "b'0'" if (r_assembly["Organella assignment file name"] =="NULL" and r_assembly["Organella assignment file MD5 code"] =="NULL" ) else "b'1'" ,
                organella_upload= None if (r_assembly["Organella assignment file name"] =="NULL" and r_assembly["Organella assignment file MD5 code"] =="NULL" ) else True ,
                plasmid_assigned= False if (r_assembly["Plasmid assignment file name"] =="NULL" and r_assembly["Plasmid assignment file MD5 code"] =="NULL" ) else True ,
                plasmid_filename= r_assembly["Plasmid assignment file name"],
                plasmid_file_md5= r_assembly['Plasmid assignment file MD5 code'],
                plasmid_upload= None if (r_assembly["Plasmid assignment file name"] =="NULL" and r_assembly["Plasmid assignment file MD5 code"] =="NULL" ) else True,
                # plasmid_upload= "b'0'" if (r_assembly["Plasmid assignment file name"] =="NULL" and r_assembly["Plasmid assignment file MD5 code"] =="NULL" ) else "b'1'" ,
                represent_gaps= False if ( (str(4 - int(r_assembly['Assembly level'])) == "3") or (str(4 - int(r_assembly['Assembly level'])) == "0") ) else True
            )
            # print(str(4 - r_assembly['Assembly level']))
            # print((str(4 - r_assembly['Assembly level']) == "3") or (str(4 - r_assembly['Assembly level']) == "0") )
            logger.info("Assignment insert ID {}".format(str(assignment_insert_id)))

            # files table
            files_insert_id = sqlutil.insert_table_files(
                scaffolds_from_contigs_filename= r_assembly["Contig to scaffold AGP file name"],
                scaffolds_from_contigs_file_md5=r_assembly['Contig to scaffold AGP file MD5 code'],
                annotation_filename=r_assembly['Genome annotation file name'],
                annotation_file_md5=r_assembly['Genome annotation file MD5 code'],
                assembly_level= 4 -  int(r_assembly['Assembly level']),
                chromsome_from= 0,
                is_check_processes= False if (r_assembly['Sequence Contamination QC'].upper().strip("'").strip().startswith("N")) else True,
                sequence_filename= r_assembly['Genome sequence file name'],
                sequence_file_md5=r_assembly['Genome sequence file MD5 code'],
                submission_way="ftp",
                agp_file=0 if (r_assembly["Contig to scaffold AGP file name"] =="NULL" and r_assembly["Other AGP file name"] =="NULL") else 1
            )                   
            logger.info("files insert ID {}".format(str(files_insert_id)))

            # genome compprise pl 中直接提供全NULL
            genome_comprise_id = sqlutil.insert_table_genome_comprise()
            logger.info("genome comprise ID {}".format(str(genome_comprise_id)))


            # reference table
            # 需要确认一下，是否是 unpubliesh的后边都不考虑
            if str(related_publication['Status']).strip().strip("'").strip().startswith("1"):
                reference_id = sqlutil.insert_table_reference(
                    publication_status="'unplished'",
                    same_author= True,
                    reference_title="unpublished paper" if (related_publication['Title'] =="NULL") else related_publication['Title'],
                    pages_from=related_publication['Pages from'],
                    pages_to= related_publication['Pages to'],
                    issue= related_publication['Issue'],
                    volumn= related_publication['Volume'],
                    year= related_publication['Year'],
                    journal_title= related_publication['Journal'],
                    url= related_publication['Paper URL'],
                    pubmedid= related_publication['PubMed ID']
                )
            elif str(related_publication['Status']).strip().strip("'").strip().startswith("2"):
                reference_id = sqlutil.insert_table_reference(
                    publication_status="'inpress'",
                    same_author= False ,
                    reference_title="inpress paper" if (related_publication['Title'] =="NULL") else related_publication['Title'],
                    pages_from=related_publication['Pages from'],
                    pages_to= related_publication['Pages to'],
                    issue= related_publication['Issue'],
                    volumn= related_publication['Volume'],
                    year= related_publication['Year'],
                    journal_title= related_publication['Journal'],
                    url= related_publication['Paper URL'],
                    pubmedid= related_publication['PubMed ID'],
                    publication_source="'Publication Detail Information'"
                )
                # 判断是否插入journal

            elif str(related_publication['Status']).strip().strip("'").strip().startswith("3"):
                reference_id = sqlutil.insert_table_reference(
                    publication_status="published",
                    same_author= False ,
                    reference_title= related_publication['Title'],
                    pages_from=related_publication['Pages from'],
                    pages_to= related_publication['Pages to'],
                    issue= related_publication['Issue'],
                    volumn= related_publication['Volume'],
                    year= related_publication['Year'],
                    journal_title= related_publication['Journal'],
                    url= related_publication['Paper URL'],
                    pubmedid= related_publication['PubMed ID'],
                    publication_source="'Publication Detail Information'"
                )

            else:
                logger.error("Publication Status error!, record is : {}".format(str(related_publication['Status']).strip()))
                # automail.mail2manager("Publication Status error!, record is : {}".format(str(related_publication['Status']).strip()))
                StartAndEnd.end()
            logger.info("reference ID {}".format(str(reference_id)))


            #下面开始为第二级table插入
            # reference authro table
            if str(related_publication['Status']).strip().strip("'").strip().startswith("1"):
                    
                reference_author_list = self.__split_publication_author_to_list(r_assembly['Sequence authors name'])
                for k in reference_author_list:
                    reference_author_id = sqlutil.insert_table_reference_author(
                        email= k['email'],
                        first_name= k['firstName'],
                        middle_name=k['middleName'],
                        last_name= k['lastName'],
                        reference_id=reference_id,
                        type=0    
                    )
            elif str(related_publication['Status']).strip().strip("'").strip().startswith("2") or str(related_publication['Status']).strip().strip("'").strip().startswith("3"):
                reference_author_list = self.__split_publication_author_to_list(related_publication['Authors'])
                for k in reference_author_list:
                    reference_author_id = sqlutil.insert_table_reference_author(
                        email= k['email'],
                        first_name= k['firstName'],
                        middle_name=k['middleName'],
                        last_name= k['lastName'],
                        reference_id=reference_id,
                        type=0  
                    )
            logger.info("reference author ID {}".format(str(reference_author_id)))

            # author table # assembly seq author
            author_list = self.__split_publication_author_to_list(r_assembly['Sequence authors name'])
            for k in author_list:
                author_id = sqlutil.insert_table_author(
                    email= k['email'],
                    first_name= k['firstName'],
                    middle_name=k['middleName'],
                    last_name= k['lastName'],
                    reference_id=reference_id,
                    type=0      
                )
            logger.info("author ID {}".format(str(author_id)))

            # contact person
            contact_author_list = self.__split_publication_author_to_list(r_assembly['Contact authors name'])
            for k in contact_author_list:
                contact_author_id = sqlutil.insert_table_contact_person(
                    email= k['email'],
                    first_name= k['firstName'],
                    middle_name=k['middleName'],
                    last_name= k['lastName'],
                    reference_id=reference_id,
                    type=0      
                )
            logger.info("contact author ID {}".format(str(contact_author_id)))

            # general info table 
            # print("*********************generalinfo")
            # print([i.strip() for i in str(r_assembly['Genome composition']).strip("'").split(";")])
            general_info_id = sqlutil.insert_table_general_info(
                assembly_name=r_assembly['Assembly name'],
                bio_project_accession=r_assembly['Biosproject Accession'],
                bio_sample_accession=r_assembly['Biosample Accession'],
                de_novo_assembly=True if (r_assembly['Reference assembly name or accession']=="NULL") else False,
                final_version=False,
                #genome_coverage=r_assembly['Genome coverage'], # 这里在pl脚本中为空，那么也保持一致
                genome_accession=r_assembly['Existing genome accession'],
                is_chloroplast=True if ("5" in [i.strip() for i in str(r_assembly['Genome composition']).strip("'").split(";")]) else False,
                is_full_genome= True if ("1" in [i.strip() for i in str(r_assembly['Genome composition']).strip("'").split(";")]) else False,
                is_mithochondria= True if ("4" in [i.strip() for i in str(r_assembly['Genome composition']).strip("'").split(";")]) else False,
                is_nuclear= True if ("2" in [i.strip() for i in str(r_assembly['Genome composition']).strip("'").split(";")]) else False,
                is_plasmid=True if ("3" in [i.strip() for i in str(r_assembly['Genome composition']).strip("'").split(";")]) else False,
                reference_assembly=r_assembly['Reference assembly name or accession'],
                release_date= r_assembly["Release date"].strftime("%Y-%m-%d %H:%M:%S") ,
                repersent=0 if ("1" in [i.strip() for i in str(r_assembly['Genome composition']).split(";")]) else 1 ,
                # sequencing_technology=r_assembly['Sequencing technology'],
                submissoin_title=r_assembly['Submission title'],
                update_submission=False if (r_assembly['Existing genome accession']=="NULL") else True,
                genome_comprise_id=str(genome_comprise_id)
                )
            logger.info("general info ID {}".format(general_info_id))
            # submission table


            submission_id = sqlutil.insert_table_submission(
                accession=this_GWHID,
                create_time=  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ,
                update_time=  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ,
                flag=0,
                is_delete=False,
                is_release_immediately=False,
                is_released=False,
                mission_id= this_WGSID,
                sub_status=6,
                assignment_id= assignment_insert_id,
                files_id=files_insert_id,
                general_info_id= general_info_id,
                reference_id=reference_id,
                submitter_id=submitter_insert_id,
                user_id=self.userid,
                is_single_contig=True if this_ID['is_single'] else False # 暂时手工指定,update 已经更改为自动
            )
            logger.info("submission ID {}".format(submission_id))

            # assembly method version 
            assembly_list = self.__split_method_version_technology_coverage_to_list(r_assembly['Assembly method'])
            version_list = self.__split_method_version_technology_coverage_to_list(r_assembly['Program Version or release date'])
            for  ind,am in enumerate(assembly_list):
                query_seq_tech = sqlutil.select_assembly_method(am)
                # print("am " + am)
                if query_seq_tech == None:
                    am_name = "Other"
                    am_id = 15
                    assembely_method_version_id = sqlutil.insert_table_assembly_method_version(
                    methodversion =  str(version_list[ind]) ,
                    other_assembly_method =str( am) ,
                    assembly_method_id= am_id,
                    general_info_id= general_info_id
                    )
                else:
                    am_name = query_seq_tech[1]
                    am_id = query_seq_tech[0]
                    assembely_method_version_id = sqlutil.insert_table_assembly_method_version(
                    methodversion = str( version_list[ind] ),
                    assembly_method_id = am_id,
                    general_info_id= general_info_id
                    )
                
            logger.info("insert assembly method and version ID {}".format(assembely_method_version_id))

            # technology and coverage
            seq_tech_list = self.__split_method_version_technology_coverage_to_list(r_assembly['Sequencing technology'])
            coverage_list = self.__split_method_version_technology_coverage_to_list(r_assembly['Genome coverage'])
            for  ind,tech in enumerate(seq_tech_list):
                query_seq_tech = sqlutil.select_technology_coverage(tech)
                # print(query_seq_tech)
                if query_seq_tech == None:
                    tech_name = "Other"
                    tech_id = 12
                    seqtech_coverage_id = sqlutil.insert_table_technology_and_coverage(
                    genome_coverage = coverage_list[ind] ,
                    other_technology = r_assembly['Sequencing technology'],
                    technology_id = tech_id ,
                    general_info_id= general_info_id
                    )
                else:
                    tech_name = query_seq_tech[1]
                    tech_id = query_seq_tech[0]
                    seqtech_coverage_id = sqlutil.insert_table_technology_and_coverage(
                    genome_coverage = coverage_list[ind] ,
                    technology_id =  tech_id ,
                    general_info_id = general_info_id
                    )
                
            logger.info("insert technology and coverage ID {}".format(seqtech_coverage_id))

            # new genome
            select_new_genome_id = sqlutil.select_new_genome(str(this_ID['taxid']))
            if not select_new_genome_id :
                # print(this_ID['taxid'])
                insert_new_genome_id = sqlutil.insert_table_new_genome_2(
                    common_name= "[]" if this_ID['commNames'] == "NULL" else   "[" + pymysql.converters.escape_string(this_ID['commNames']) +"]"  ,
                    synonym_names= "[]" if this_ID['synonymNames'] == "NULL" else   "[" + this_ID['synonymNames'] +"]",
                    created_at=  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ,
                    scientific_name=  this_ID['samplename'] ,
                    taxon_id= this_ID['taxid'],
                    status= 0,
                    gen_bank_common_name= this_ID['genebankname'],
                )
            else:
                insert_new_genome_id =  select_new_genome_id[0]

            logger.info("insert new genome ID {}".format(insert_new_genome_id))

            # new assembly 
            assemb_methods_union =""
            for k,am in enumerate(assembly_list):
                u = am + " " + version_list[k]
                if k > 0:
                    assemb_methods_union += "<br>" +  u 
                else:
                    assemb_methods_union += u
           
            tech_cov_union =""
            for m,tech in enumerate(seq_tech_list):
                u = tech + " " + coverage_list[m]
                if m > 0:
                    tech_cov_union += "<br>" + u
                else:
                    tech_cov_union += u
           

            sample_info = self.apis.getSampleInfo(r_assembly['Biosample Accession'].strip("'"))

            # 将new assemlby 中的project accession 修改为末尾8个0
            assembly_proj_accession = this_GWHID[:7] + "00000000"
            
            # new_assembly_contacts = "'" + r_assembly['Sequence authors name'].strip("'").strip(";").replace(";","<br>") +"'"
            new_assembly_contacts =  re.sub(r"\)","",re.sub( r" *\(","   ",r_assembly['Sequence authors name'].strip("'").strip(";").replace(";","<br>"))) 
            # new_assembly_reference_cantacts =  "'" + r_assembly['Contact authors name'].strip("'").strip(";").replace(";","<br>") +"'"
            new_assembly_reference_cantacts =   re.sub(r"\)","",re.sub( r" *\(","   ",r_assembly['Contact authors name'].strip("'").strip(";").replace(";","<br>"))) 

            self.__genPaperCitation(related_publication)
            if str(related_publication['Status']).strip().strip("'").strip().startswith("1"):
                insert_new_assembly_id = sqlutil.insert_table_new_assembly(
                    bio_project_accession=r_assembly['Biosproject Accession'],
                    bio_sample_accession=r_assembly['Biosample Accession'],
                    accession= this_GWHID ,
                    assembly_project_accession=assembly_proj_accession ,
                    assembly_level=(4 -  int(r_assembly['Assembly level'])),
                    chloroplast=True if ("5" in [i.strip() for i in str(r_assembly['Genome composition']).split(";")]) else False,
                    mitochondrion=True if ("4" in [i.strip() for i in str(r_assembly['Genome composition']).split(";")]) else False,
                    qc=False if (r_assembly['Sequence Contamination QC'].upper().strip("'").strip().startswith("N")) else True,
                    sample_source="'GWH'",
                    source="'GWH'",
                    country=related_contact["Country/Region"],
                    assemblymethods= assemb_methods_union ,
                    technology_and_coverage=  tech_cov_union ,
                    genome_representation="0" if ("1" in [i.strip() for i in str(r_assembly['Genome composition']).split(";")]) else "1",
                    name = r_assembly['Assembly name'],
                    jbrowse_data_available= False,
                    # lab = "'" + related_contact['Organization'].strip("'") + "," + related_contact['Department'].strip("'") + "'",
                    lab = pymysql.converters.escape_string( related_contact['Department'].strip('"') + ", " +   related_contact['Organization'].strip('"') ),
                    is_released=False,
                    released_at= r_assembly["Release date"].strftime("%Y-%m-%d %H:%M:%S") , # 这里日期不标准，则会导致报错。
                    type = sample_info['type'] ,
                    sample_name= sample_info['samplename'],
                    contacts= new_assembly_contacts,
                    reference_contacts= new_assembly_reference_cantacts,
                    papers="-",
                    paper_journal=related_publication['Journal'],
                    paper_pubmed_id=related_publication['PubMed ID'],
                    paper_other_links=related_publication['Paper URL'],
                    paper_is_supported_by_gwh=False,
                    is_controlled= True if (is_control) else False,
                    new_genome_id= insert_new_genome_id
                )
            if str(related_publication['Status']).strip().strip("'").strip().startswith("2") or str(related_publication['Status']).strip().strip("'").strip().startswith("3") :
                
                insert_new_assembly_id = sqlutil.insert_table_new_assembly(
                    bio_project_accession=r_assembly['Biosproject Accession'],
                    bio_sample_accession=r_assembly['Biosample Accession'],
                    accession=this_GWHID ,
                    assembly_project_accession=assembly_proj_accession ,
                    assembly_level=(4 - int(r_assembly['Assembly level'])),
                    chloroplast=True if ("5" in [i.strip() for i in str(r_assembly['Genome composition']).split(";")]) else False,
                    mitochondrion=True if ("4" in [i.strip() for i in str(r_assembly['Genome composition']).split(";")]) else "b'0'",
                    qc="b'0'" if (r_assembly['Sequence Contamination QC'].upper().strip("'").strip().startswith("N")) else True,
                    sample_source="'GWH'",
                    source="'GWH'",
                    country=related_contact["Country/Region"],
                    assemblymethods= assemb_methods_union ,
                    technology_and_coverage=  tech_cov_union ,
                    genome_representation="b'0'" if ("1" in [i.strip() for i in str(r_assembly['Genome composition']).split(";")]) else True,
                    name = r_assembly['Assembly name'],
                    jbrowse_data_available=False,
                    lab = pymysql.converters.escape_string( related_contact['Department'].strip('"') + ", " +   related_contact['Organization'].strip('"') ) ,
                    # lab = "'" + related_contact['Organization'].strip("'") + "," + related_contact['Department'].strip("'") + "'",
                    is_released=False,
                    released_at=  r_assembly["Release date"].strftime("%Y-%m-%d %H:%M:%S") ,
                    type =  sample_info['type'] ,
                    sample_name=  sample_info['samplename'] ,
                    contacts= new_assembly_contacts,
                    reference_contacts= new_assembly_reference_cantacts,
                    papers="-",
                    paper_journal=related_publication['Journal'],
                    paper_pubmed_id=related_publication['PubMed ID'],
                    paper_other_links=related_publication['Paper URL'],
                    paper_is_supported_by_gwh=False,
                    is_controlled= True if (is_control) else False ,
                    new_genome_id=insert_new_genome_id
                )

            
            logger.info("insert new assembly ID {}".format(insert_new_assembly_id))

            # insrt into new
            new_assembly_size = None
            # print(this_ID.keys())
            if "assembly.stats" in this_ID.keys():
                # insert into 
                for k,v in this_ID["assembly.stats"].items():

                    # update size  to new assembly 
                   
                    if k.strip() == "Genome size (bp)":
                        
                        new_assembly_size = v
                    # insert into new_stats 
                    sqlutil.insert_table_new_statistic(str(k),str(v),insert_new_assembly_id)
                logger.info("insert new statistic ")
            elif "detail.stats" in this_ID.keys():
                # insert into
                for k,v in this_ID["detail.stats"].items():
                    sqlutil.insert_table_new_annotation(
                        name=str(k),
                        ncrna=str( v[0]),
                        others=str(v[1]),
                        protein=str(v[2]),
                        pseudogene=str(v[3]),
                        rrna=str(v[4]),
                        trna=str(v[5]),
                        total=str(v[6]),
                        new_assembly_id= insert_new_assembly_id
                        )
                logger.info("insert new annotation ")
            
            elif "whole.stats" in this_ID.keys():
                # insert into
                for k,v in this_ID["whole.stats"].items():
                    sqlutil.insert_table_new_annotation_detail(
                        name=str(k),
                        ncrna=str(v[0]),
                        others=str(v[1]),
                        protein=str(v[2]),
                        pseudogene=str(v[3]),
                        rrna=str(v[4]),
                        trna=str(v[5]),
                        total=str(v[6] ),
                        new_assembly_id= insert_new_assembly_id
                        )
                logger.info("insert new annotation detail")

            if new_assembly_size ==None:
                logger.warn("Genome Size is NULL...")
            else:
                sqlutil.update_size_into_new_assembly(new_assembly_size.replace(",",""),insert_new_assembly_id)
                logger.info("update genome size in new_assembly record(ID {})successfully.".format(insert_new_assembly_id))
        
        for r_publication in publicationojb:
            
            hits = sqlutil.select_journal(r_publication['Journal'])
            if hits:
                continue
            else:
                # insert new journal record
                insert_journal_id = sqlutil.insert_table_journal(
                    journal_name = r_publication['Journal']
                )
                logger.info("insert journal id {}".format(insert_journal_id))
            
            
        sqlutil.commit()

        # except Exception as e :
        #     logger.info("Insert into database found transaction Error: {},rollback...".format(str(e)))
            
        #     sqlutil.rollback()
        #     is_succeed = False 
        #     sqlutil.close()
        #     automail.mail2manager("Insert into database found transaction Error: {},rollback...".format(str(e)))
        #     StartAndEnd.end()
        # finally:
        #     sqlutil.close()

            

        return([is_succeed,None]) # none 是错误信息占位符


    # def insertExcel2JournalTable(self,publicationojb):
    #     """Journal table 与其他表格都不关联，所以可以单独插入"""
    #     sqlutil = Mysql.mysqlUtils()
        
        
    #     for r_publication in publicationojb:
    #         try:
    #             hits = sqlutil.select_journal(r_publication['Journal'])
    #             if hits:
    #                 continue
    #             else:
    #                 # insert new journal record
    #                 insert_journal_id = sqlutil.insert_table_journal(
    #                     journal_name = r_publication['Journal']
    #                 )
    #                 logging.info("insert journal id {}".format(insert_journal_id))
    #             is_succeed  = True
    #             sqlutil.commit()
    #         except Exception as e :
    #             logging.info("Transaction Error: {}".format(e))
    #             sqlutil.rollback()
    #             logging.info("Rollback successfully.")
    #             is_succeed = False 
    #             sqlutil.close()
    #             break
            
    #         finally:
    #             sqlutil.close()

    #     return([is_succeed,None])




    def __split_publication_author_to_list(self,a):
        """publication 列的author分割到list中"""
        b = a.strip().split(";")
        total = []
        for r in b:
            # print(r)
            out = {"firstName":"NULL","middleName":"NULL","lastName":"NULL","email":"NULL"}
            r = r.strip().strip("'").strip()
            # l =[j.strip() for j in  re.split("( +|\()",r)]

            spl = r.split("(")
            if len(spl) == 1:
                names = re.split(" +",spl[0])
            elif len(spl) ==2:
                names = re.split(" +",spl[0])
                if "@" in spl[1]:
                    email = str( spl[1].strip("'").strip(")").strip("(") )
            clean_names = [i for i in names if i != ""]

            try:
                if len(clean_names) == 2:
                    out['firstName'] = str( clean_names[0] )
                    out['lastName'] = str( clean_names[1] )
                    out['email'] = email
                elif len(clean_names) ==  3:
                    out['firstName'] = str( clean_names[0] )
                    out['middleName'] =str( clean_names[1] )
                    out['lastName'] = str( clean_names[2] )
                    out['email'] = email
            except:
                pass

            # logging.info(out)
            total.append(out)
        return(total)

    def __split_method_version_technology_coverage_to_list(self,a):
        b = str(a).strip().strip("'").strip().split(";")
        out = []
        for k in b:
            if not re.match("^ +$",k):
                out.append(k)
        # print("split_assembly_method {}".format(",".join(out)))
        return(out)


    def __genPaperCitation(self,publicatoinobj):
        # pub_author = self.__split_publication_author_to_list(publicatoinobj['Authors'])
        # 优化
        " ".join([publicatoinobj['Authors'].strip("'"),
        publicatoinobj['Title'].strip("'"),publicatoinobj['Volume'].strip("'"),
        "("+publicatoinobj['Issue'].strip("'")+")",publicatoinobj['Pages from'].strip("'"),
        publicatoinobj['Pages to'].strip("'")
         ])




if __name__=="__main__":
    pass
    # conf = parseConfig.configs("./Config.txt")

    # excel  = exportExcel2Table.Excel2Objects("./excel/210126 gwh-batchsubmission-chinese-mini.xlsx",confobj=conf)
    # excel  = exportExcel2Table.Excel2Objects(conf.get_value("EXCEL"),confobj=conf)

    # ContactObj = excel.getContactObjsList()
    # PublicatinObj = excel.getPublicationObjsList()
    # AssemblyObj = excel.getAssemblyObjsList()

    # GWHdbm = GWHdbManager()

    # IDlist = batchIDGen.getBatchGWHAndWGSDict(AssemblyObj,conf)
    # print(IDlist)
    # # GWHdbm.insertExcelInfoRecords(contactobj=ContactObj,publicationojb=PublicatinObj,IDlist=IDlist,assemblyobj=AssemblyObj)
    # GWHdbm.updateGWHIDandWGSIDtoDB(IDlist)

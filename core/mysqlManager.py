#!/usr/bin/env python3



import pymysql
import os
from core.workerGlobal import gConfigs
from core.workerGlobal import gMailManager as automail
from core.workerGlobal import gLog2File as logger
import core.eachJobGlobalVars  as gvar
from core.workerGlobal import StartAndEnd

class mysqlUtils():

    def __init__(self):

        if gvar.RuntimeConfig.getRunMode() == "test":
        # if True:
            self.host = gConfigs.get("mysql_test","DB_HOST")
            self.port = gConfigs.get("mysql_test","DB_PORT")
            self.username =  gConfigs.get("mysql_test","DB_USERNAME")
            self.password =  gConfigs.get("mysql_test","DB_PASSWORD")
            self.dbname = gConfigs.get("mysql_test","DB_DBNAME")
            self.charsets =  gConfigs.get("mysql_test","DB_CHARSETS")
        else:
            self.host = gConfigs.get("mysql_prod","DB_HOST")
            self.port = gConfigs.get("mysql_prod","DB_PORT")
            self.username =  gConfigs.get("mysql_prod","DB_USERNAME")
            self.password =  gConfigs.get("mysql_prod","DB_PASSWORD")
            self.dbname = gConfigs.get("mysql_prod","DB_DBNAME")
            self.charsets =  gConfigs.get("mysql_prod","DB_CHARSETS")

        self.base_header = "INSERT INTO {}.".format(self.dbname)
        try:
            self.con = pymysql.Connect(
            host=self.host,
            port = int(self.port),
            user=self.username,
            passwd = self.password,
            db=self.dbname,
            charset=self.charsets
            )
            #获得数据库的游标
            self.cursor = self.con.cursor() #开启事务
            logger.info("Get cursor successfully")
        except Exception as e :
            logger.info("Can not connect databse {}\nReason:{}".format(self.dbname,e))
    def close(self):
        if  self.con:
            self.con.commit()
            self.con.close()
            logger.info("Close database {} successfully".format(self.dbname))
        else:
            logger.info("DataBase doesn't connect,close connectiong error;please check the db config.")


    def fetchOne(self):
        self.con.ping(reconnect=True)
        data = self.cursor.fetchone()
        return(data)


    def excute(self,sql,args=None):
    
        if args == None:
            logger.info(sql)
            # logger.info(repr(sql))
            self.con.ping(reconnect=True)
            self.cursor.execute(sql)
            return(self.cursor.rowcount)
        else:
            logger.info(sql)
            logger.info(str(args))
            # logger.info(repr(sql))
            self.con.ping(reconnect=True)
            self.cursor.execute(sql,args)
            return(self.cursor.rowcount)     

    def excute_insert(self,sql,myargs):
        # sql2 =  pymysql.converters.escape_string(sql) # pymysql version must older than 1.0; from pymysql import escape_string is used for version under 1.0
        # logging.info(sql+"\nvalues:"+",".join(list(myargs)))
        logger.info(sql)
        self.con.ping(reconnect=True)
        self.cursor.execute(sql,myargs)
        return(self.cursor.rowcount)

    def commit(self):
        self.con.commit()
    def select_country(self,country):
        sql = "SELECT country_id FROM country WHERE UPPER(country_name) = UPPER(%s)" #.format(country.strip().strip("'").strip())
        self.excute(sql,(country.strip().strip("'").strip(),))
        return(self.fetchOne())

    def select_journal(self,journal):
        sql = "SELECT id FROM journal WHERE UPPER(journal_name) = UPPER(%s)"#.format(journal.strip().strip("'").strip())
        # sql = "SELECT id FROM `journal` WHERE journal_name = "NATIONAL Science Review";".format(journal.strip().strip("'").strip())
        self.excute(sql,(journal.strip().strip("'").strip(),))
        return(self.fetchOne())


    def select_assembly_method(self,method):
        sql = "SELECT id,name FROM assembly_method WHERE UPPER(name) = UPPER(%s)" #.format(method.strip().strip("'").strip())
        self.excute(sql,(method.strip().strip("'").strip(),))
        return(self.fetchOne())

    def select_new_genome(self,taxid):
        taxid = str(taxid)
        sql = "SELECT id FROM `new_genome` WHERE taxon_id = %s;" #.format(taxid.strip().strip("'").strip())
        self.excute(sql,(taxid.strip().strip("'").strip(),))
        return(self.fetchOne())

    def select_technology_coverage(self,technology):
        sql = "SELECT id,name FROM technology WHERE UPPER(name) = UPPER(%s);" #.format(technology.strip().strip("'").strip())
        self.excute(sql,(technology.strip().strip("'").strip(),))
        return(self.fetchOne())

    def select_max_ID_from_databasemeta(self):
        sqlGWHID = "select metadata_value FROM database_metadata WHERE metadata_key = 'last_accession';"
        sqlWGSID = "select metadata_value FROM database_metadata WHERE metadata_key = 'last_submission_mission_id';"
        self.excute(sqlGWHID)
        GWHID = self.fetchOne() 
        self.excute(sqlWGSID)
        WGSID = self.fetchOne() 
        return({"max_accession_id":GWHID[0],"max_submission_mission_id":WGSID[0]})


    def update_max_ID_to_table_databasemeta(self,maxWGS,maxGWH):
        try:
            sql1 = "UPDATE database_metadata SET `metadata_value` = %s WHERE metadata_key = 'last_accession';"
            #.format(maxGWH.strip().strip("'").strip())
            sql2 = "UPDATE database_metadata SET `metadata_value` = %s  WHERE metadata_key = 'last_submission_mission_id';"
            #.format(maxWGS.strip().strip("'").strip())
            # logging.info(sql1)
            # logging.info(sql2)
            self.excute(sql1,maxGWH.strip().strip("'").strip())
            self.excute(sql2,maxWGS.strip().strip("'").strip())
            return(True)
        except Exception as e :
            logger.error("Can not update_max_ID_to_table_databasemeta reason:\n " +str(e))
            return(False)



    def update_size_into_new_assembly(self,size=None,new_assembly_id=None):
        if not new_assembly_id:
            raise Exception("update_size_new_assembly: new_assembly id must no t null")
        try:
            # print(size)
            # print(new_assembly_id)
            sql = "UPDATE new_assembly SET size=%s WHERE id = %s;"
            # .format(
            #     str(size).strip().strip("'").strip(),
            #     str(new_assembly_id).strip().strip("'").strip()
            #     )
            # logging.info(sql)
            self.excute(sql,(str(size).strip().strip("'").strip(),str(new_assembly_id).strip().strip("'").strip()))
            return(True)
        except:
            return(False)

        
    def insert_table_journal(self,journal_name=None,image_path=None,show=False):
        sql = self.base_header + "`journal`(`journal_name`,`img_path_filename`,`show`) VALUES(%s,%s,%s);"
        args =(
            journal_name,image_path,show )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())        

    def insert_table_genome_comprise(self,chloroplast=None, chromosome=None, mithochondria=None, other_comprise=None, plasmid=None, ploidy =None):
        sql = self.base_header + "`genome_comprise`(`chloroplast`, `chromosome`, `mithochondria`, `other_comprise`, `plasmid`, `ploidy`) VALUES ( %s, %s, %s, %s, %s, %s);"
        args = (
            chloroplast, chromosome, mithochondria, other_comprise, plasmid, ploidy )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())

    def insert_table_files(self, scaffolds_from_contigs_file_md5=None, scaffolds_from_contigs_filename=None, scaffolds_from_contigs_filepath=None, annotation_file_md5=None, annotation_filename=None, annotation_filepath=None, assembly_level=None, chro_from_contigs_file_md5=None, chro_from_contigs_filename=None, chro_from_contigs_filepath=None, chro_from_scaffolds_file_md5=None, chro_from_scaffolds_filename=None, chro_from_scaffolds_filepath=None, chromsome_from=None, genetic_code=None, is_check_processes=None, sequence_file_md5=None, sequence_filename=None, sequence_filepath=None, submission_way=None, agp_file =None):
        "not null assembly_level=,chromsome_from=,is_check_processes=,agp_file="
        sql = self.base_header + ("`files`(`scaffolds_from_contigs_file_md5`,"
                                    " `scaffolds_from_contigs_filename`, "
                                    "`scaffolds_from_contigs_filepath`, "
                                    "`annotation_file_md5`, "
                                    "`annotation_filename`, "
                                    "`annotation_filepath`, "
                                    "`assembly_level`, "
                                    "`chro_from_contigs_file_md5`, "
                                    "`chro_from_contigs_filename`, "
                                    "`chro_from_contigs_filepath`, "
                                    "`chro_from_scaffolds_file_md5`, "
                                    "`chro_from_scaffolds_filename`, "
                                    "`chro_from_scaffolds_filepath`,"
                                    " `chromsome_from`,"
                                    " `genetic_code`,"
                                    " `is_check_processes`, "
                                    "`sequence_file_md5`, "
                                    "`sequence_filename`, "
                                    "`sequence_filepath`, "
                                    "`submission_way`, "
                                    "`agp_file` "
                                    ") VALUES ("
                                    " %s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s);")
        args =(
                scaffolds_from_contigs_file_md5, scaffolds_from_contigs_filename, scaffolds_from_contigs_filepath, annotation_file_md5, annotation_filename,
                annotation_filepath, assembly_level, chro_from_contigs_file_md5, chro_from_contigs_filename, chro_from_contigs_filepath, chro_from_scaffolds_file_md5, 
                chro_from_scaffolds_filename, chro_from_scaffolds_filepath, chromsome_from, genetic_code, is_check_processes, sequence_file_md5, sequence_filename, 
                sequence_filepath, submission_way, agp_file    
                )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())


    def insert_table_assembly_method_version(self, methodversion=None, other_assembly_method=None, standard_other_assembly_method=None, assembly_method_id=None, general_info_id =None):
        sql = self.base_header + "`assembly_method_version`(`methodversion`, `other_assembly_method`, `standard_other_assembly_method`, `assembly_method_id`, `general_info_id`) VALUES ( %s, %s, %s, %s, %s);"
        args =(
            methodversion, other_assembly_method, standard_other_assembly_method, assembly_method_id, general_info_id
        )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())

    def insert_table_assignment(self,chromosome_assigned=None, chromosome_file_md5=None, chromosome_filename=None, chromosome_upload=None, gap_length_type=None, gap_type=None, minimum_gap_length=None, organella_assigned=None, organella_file_md5=None, organella_filename=None, organella_upload=None, plasmid_assigned=None, plasmid_file_md5=None, plasmid_filename=None, plasmid_upload=None, represent_gaps =None):
        sql = self.base_header + "`assignment`( `chromosome_assigned`, `chromosome_file_md5`, `chromosome_filename`, `chromosome_upload`, `gap_length_type`, `gap_type`, `minimum_gap_length`, `organella_assigned`, `organella_file_md5`, `organella_filename`, `organella_upload`, `plasmid_assigned`, `plasmid_file_md5`, `plasmid_filename`, `plasmid_upload`, `represent_gaps`) VALUES ( %s, %s, %s, %s, %s, %s,%s,%s, %s, %s,%s,%s, %s, %s,%s,%s);" 
        args = (
            chromosome_assigned, chromosome_file_md5, chromosome_filename, chromosome_upload, gap_length_type, gap_type, minimum_gap_length, organella_assigned, organella_file_md5, organella_filename, organella_upload, plasmid_assigned, plasmid_file_md5, plasmid_filename, plasmid_upload, represent_gaps 
        )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())

    def insert_table_author(self,email=None, first_name=None, last_name=None, middle_name=None, type=None, reference_id =None):
        sql = self.base_header + "`author`(`email`, `first_name`, `last_name`, `middle_name`, `type`, `reference_id`) VALUES ( %s, %s, %s, %s, %s, %s);"
        args = (
            email, first_name, last_name, middle_name, type, reference_id 
        )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())

    def insert_table_contact_person(self, email=None, first_name=None, last_name=None, middle_name=None, type=None, reference_id=None, reference_contacts =None):
        sql = self.base_header + "`contact_person`( `email`, `first_name`, `last_name`, `middle_name`, `type`, `reference_id`, `reference_contacts`) VALUES ( %s, %s, %s, %s, %s, %s,%s);"
        args = (
            email, first_name, last_name, middle_name, type, reference_id, reference_contacts
        )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())        


    def insert_table_general_info(self,assembly_date=None, assembly_method=None, assembly_name=None, bio_project_accession=None, bio_sample_accession=None, de_novo_assembly=None, final_version=None, genome_accession=None, genome_coverage=None, is_chloroplast=None, is_full_genome=None, is_mithochondria=None, is_nuclear=None, is_plasmid=None, msg_to_staff=None, reads_accession=None, reference_assembly=None, release_date=None, repersent=None, repersent_others=None, sequencing_technology=None, submissoin_title=None, update_submission=None, genome_comprise_id =None):
        sql = self.base_header + "`general_info`(`assembly_date`, `assembly_method`, `assembly_name`, `bio_project_accession`, `bio_sample_accession`, `de_novo_assembly`, `final_version`, `genome_accession`, `genome_coverage`, `is_chloroplast`, `is_full_genome`, `is_mithochondria`, `is_nuclear`, `is_plasmid`, `msg_to_staff`, `reads_accession`, `reference_assembly`, `release_date`, `repersent`, `repersent_others`, `sequencing_technology`, `submissoin_title`, `update_submission`, `genome_comprise_id`) VALUES ( %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s);"
        args = (
           assembly_date, assembly_method, assembly_name, bio_project_accession, bio_sample_accession, de_novo_assembly, final_version, genome_accession, genome_coverage, is_chloroplast, is_full_genome, is_mithochondria, is_nuclear, is_plasmid, msg_to_staff, reads_accession, reference_assembly, release_date, repersent, repersent_others, sequencing_technology, submissoin_title, update_submission, genome_comprise_id 
        )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())


    def insert_table_new_assembly(self,dna=None, gff=None, protein=None, rna=None, technology_and_coverage=None, assembly_project_accession=None, accession=None, ancestor_assembly_accessions=None, assembly_date=None, assembly_level=None, assemblymethods=None, bio_project_accession=None, bio_sample_accession=None, chloroplast=None, chr=None, contacts=None, copy_number=None, country=None, description=None, genome_representation=None, jbrowse_data_available=None, lab=None, mitochondrion=None, name=None, other_component=None, papers=None, paper_journal=None, paper_pubmed_id=None, paper_other_links=None, paper_published_date=None, paper_is_supported_by_gwh=None, plasmid_num=None, qc=None, reads_accession=None, released_at=None, sample_name=None, sample_source=None, sequencing_technology=None, size=None, source=None, supplementary_url=None, version=None, new_genome_id=None, is_released=None, ftp_dir=None, cds=None, feature=None, type=None, is_controlled=None, reference_contacts =None):
        sql = self.base_header + "`new_assembly`(`dna`, `gff`, `protein`, `rna`, `technology_and_coverage`, `assembly_project_accession`, `accession`, `ancestor_assembly_accessions`, `assembly_date`, `assembly_level`, `assemblymethods`, `bio_project_accession`, `bio_sample_accession`, `chloroplast`, `chr`, `contacts`, `copy_number`, `country`, `description`, `genome_representation`, `jbrowse_data_available`, `lab`, `mitochondrion`, `name`, `other_component`, `papers`, `paper_journal`, `paper_pubmed_id`, `paper_other_links`, `paper_published_date`, `paper_is_supported_by_gwh`, `plasmid_num`, `qc`, `reads_accession`, `released_at`, `sample_name`, `sample_source`, `sequencing_technology`, `size`, `source`, `supplementary_url`, `version`, `new_genome_id`, `is_released`, `ftp_dir`, `cds`, `feature`, `type`, `is_controlled`, `reference_contacts`) VALUES ( %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s);"
        args =(
            dna, gff, protein, rna, technology_and_coverage, assembly_project_accession, accession, ancestor_assembly_accessions, assembly_date, assembly_level, assemblymethods, bio_project_accession, bio_sample_accession, chloroplast, chr, contacts, copy_number, country, description, genome_representation, jbrowse_data_available, lab, mitochondrion, name, other_component, papers, paper_journal, paper_pubmed_id, paper_other_links, paper_published_date, paper_is_supported_by_gwh, plasmid_num, qc, reads_accession, released_at, sample_name, sample_source, sequencing_technology, size, source, supplementary_url, version, new_genome_id, is_released, ftp_dir, cds, feature, type, is_controlled, reference_contacts 
        )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())


    def insert_table_reference_author(self,email=None, first_name=None, last_name=None, middle_name=None, type=None, reference_id =None):
        sql = self.base_header + "`reference_author`( `email`, `first_name`, `last_name`, `middle_name`, `type`, `reference_id`) VALUES ( %s, %s, %s, %s, %s, %s);"
        args = (
            email, first_name, last_name, middle_name, type, reference_id )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())        

    def insert_table_reference(self,issue=None, journal_title=None, pages_from=None, pages_to=None, publication_source=None, publication_status=None, pubmedid=None, reference_title=None, same_author=None, url=None, volumn=None, year =None):
        sql = self.base_header + "`reference`(`issue`, `journal_title`, `pages_from`, `pages_to`, `publication_source`, `publication_status`, `pubmedid`, `reference_title`, `same_author`, `url`, `volumn`, `year`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        args = (
            issue, journal_title, pages_from, pages_to, publication_source, publication_status, pubmedid, reference_title, same_author, url, volumn, year )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())
    def insert_table_submission(self,accession=None, app=None, create_time=None, delete_time=None, flag=None, has_file=None, is_delete=None, is_release_immediately=None, is_released=None, mission_id=None, release_time=None, sub_group=None, sub_status=None, update_time=None, assignment_id=None, files_id=None, general_info_id=None, reference_id=None, submitter_id=None, user_id=None, type=None, is_single_contig =None):
        sql = self.base_header + "`submission`(`accession`, `app`, `create_time`, `delete_time`, `flag`, `has_file`, `is_delete`, `is_release_immediately`, `is_released`, `mission_id`, `release_time`, `sub_group`, `sub_status`, `update_time`, `assignment_id`, `files_id`, `general_info_id`, `reference_id`, `submitter_id`, `user_id`, `type`, `is_single_contig` ) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        args =(
            accession, app, create_time, delete_time, flag, has_file, is_delete, is_release_immediately, is_released, mission_id, release_time, sub_group, sub_status, update_time, assignment_id, files_id, general_info_id, reference_id, submitter_id, user_id, type, is_single_contig  )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())

    #modified
    def insert_table_submitter(self,account_non_expired=None, account_non_locked=None, active_time=None, city=None, create_time=None, credentials_non_expired=None, delete_time=None, department=None, standard_department=None, email=None, enabled=None, fax=None, first_name=None, last_name=None, middle_name=None, modify_time=None, organization=None, standard_organization=None, organization_url=None, phone=None, postal_code=None, secondary_email=None, state=None, street=None, country_id =None):
        sql = self.base_header + "`submitter`( `account_non_expired`, `account_non_locked`, `active_time`, `city`, `create_time`, `credentials_non_expired`, `delete_time`, `department`, `standard_department`, `email`, `enabled`, `fax`, `first_name`, `last_name`, `middle_name`, `modify_time`, `organization`, `standard_organization`, `organization_url`, `phone`, `postal_code`, `secondary_email`, `state`, `street`, `country_id`) VALUES ( %s, %s, %s, %s, %s,%s, %s, %s,%s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s);"
        args = (account_non_expired, account_non_locked, active_time, city, create_time, credentials_non_expired, delete_time, department, standard_department, email, enabled, fax, first_name, last_name, middle_name, modify_time, organization, standard_organization, organization_url, phone, postal_code, secondary_email, state, street, country_id  )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())


    def insert_table_technology_and_coverage(self,genome_coverage=None, other_technology=None, general_info_id=None, technology_id =None):  
        sql = self.base_header + "`technology_and_coverage`(`genome_coverage`, `other_technology`, `general_info_id`, `technology_id` ) VALUES ( %s, %s, %s, %s);"
        args = (
            genome_coverage, other_technology, general_info_id, technology_id )
        # logging.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())              


    # def insert_table_new_genome(self, common_name="'[]'", created_at=None, description=None, ebi_url=None, jgi_url=None, ncbi_id=None, ncbi_url=None, plaza_url=None, sample_picture=None, scientific_name=None, source=None, sp_db_url=None, taxon_id=None, status=None, gen_bank_common_name=None, synonym_names="'[]'"):
    #     sql = self.base_header + "`new_genome`(`common_name`, `created_at`, `description`, `ebi_url`, `jgi_url`, `ncbi_id`, `ncbi_url`, `plaza_url`, `sample_picture`, `scientific_name`, `source`, `sp_db_url`, `taxon_id`, `status`, `gen_bank_common_name`, `synonym_names` ) VALUES ( %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s);".format(
    #         common_name, created_at, description, ebi_url, jgi_url, ncbi_id, ncbi_url, plaza_url, sample_picture, scientific_name, source, sp_db_url, taxon_id, status, gen_bank_common_name, synonym_names )
    #     # pymysql.escape_string("''aa''")
    #     # logging.info(pymysql.converters.escape_string(sql))
    #     # self.excute_insert(sql)
    #     self.excute(sql)
    #     return(self.con.insert_id())     


    def insert_table_new_genome_2(self, common_name="[]", created_at=None, description=None, ebi_url=None, jgi_url=None, ncbi_id=None, ncbi_url=None, plaza_url=None, sample_picture=None, scientific_name=None, source=None, sp_db_url=None, taxon_id=0, status=0, gen_bank_common_name=None, synonym_names="[]"):
        # sql = self.base_header + "`new_genome`(`common_name`, `created_at`, `description`, `ebi_url`, `jgi_url`, `ncbi_id`, `ncbi_url`, `plaza_url`, `sample_picture`, `scientific_name`, `source`, `sp_db_url`, `taxon_id`, `status`, `gen_bank_common_name`, `synonym_names` ) VALUES ( %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s);".format(
            # common_name, created_at, description, ebi_url, jgi_url, ncbi_id, ncbi_url, plaza_url, sample_picture, scientific_name, source, sp_db_url, taxon_id, status, gen_bank_common_name, synonym_names )
        # pymysql.escape_string("''aa''")
        # logging.info(pymysql.converters.escape_string(sql))
        # self.excute_insert(sql)


        sql = self.base_header + "`new_genome`(`common_name`, `created_at`, `description`, `ebi_url`, `jgi_url`, `ncbi_id`, `ncbi_url`, `plaza_url`, `sample_picture`, `scientific_name`, `source`, `sp_db_url`, `taxon_id`, `status`, `gen_bank_common_name`, `synonym_names` ) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

        args =( common_name, created_at, description, ebi_url, jgi_url, ncbi_id, ncbi_url, plaza_url, sample_picture, scientific_name, source, sp_db_url, taxon_id, status, gen_bank_common_name, synonym_names )

        # print(myargs)
        # print(type(myargs))
        self.excute(sql,args)
        return(self.con.insert_id())    


    def rollback(self):
        self.con.rollback()
        logger.info("RollBack Transaction")

    # assembly stats
    def insert_table_new_statistic(self,k=None,v=None,new_assembly_id =None):
        sql = self.base_header + "`new_statistics`(`name`, `value`, `new_assembly_id` ) VALUES ( %s, %s, %s);"
        args = (
            k,v,new_assembly_id)
        # logger.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())     
    
    # whole stats
    def insert_table_new_annotation(self,name=None, ncrna=None, others=None, protein=None, pseudogene=None, rrna=None, trna=None, total=None, new_assembly_id=None):
        sql = self.base_header + "`new_statistics`(`name`, `ncrna`, `others`, `protein`, `pseudogene`, `rrna`, `trna`, `total`, `new_assembly_id` ) VALUES ( %s, %s,%s, %s,%s, %s,%s, %s,%s);"
        args = (
            name, ncrna, others, protein, pseudogene, rrna, trna, total, new_assembly_id)
        # logger.info(sql)
        self.excute(sql,args)
        return(self.con.insert_id())     
    
    # detial stats
    def insert_table_new_annotation_detail(self,name=None, ncrna=None, others=None, protein=None, pseudogene=None, rrna=None, trna=None, total=None, new_assembly_id=None):
        sql = self.base_header + "`new_annotation_detail`(`name`, `ncrna`, `others`, `protein`, `pseudogene`, `rrna`, `trna`, `total`, `new_assembly_id` ) VALUES ( %s, %s,%s, %s,%s, %s,%s, %s,%s);"
        args=(
            name, ncrna, others, protein, pseudogene, rrna, trna, total, new_assembly_id)
        logger.info(sql,args)
        self.excute(sql)
        return(self.con.insert_id())     




if __name__ == "__main__":


    sqlutil = mysqlUtils()

    sql = "select * from country"

    # 执行sql后会返回rowcount
    rc = sqlutil.excute(sql)
    # print(sqlutil.select_max_ID_from_databasemeta())    
    # 用以下代码插入

    # for i in range(rc +100):
    #     print(sqlutil.fetchOne())
    

    # 操作事务的代码
    # 一次实例是一次事务，支持出错回滚


    # try:

    #     # a=sqlutil.select_country("china")

    #     # a = sqlutil.update_max_ID_to_table_databasemeta("WGS00000","GWHACCD00000000")
    #     # if a :
    #     #     print("get return , succeed")
    #     # else:
    #     #     print("get return , failed")
        
    #     # print(sqlutil.insert_table_genome_comprise(mithochondria=1))
    #     # print(sqlutil.insert_table_files(assembly_level=1,chromsome_from=1,is_check_processes=1,agp_file=1))
    #     # # "not null assembly_level=,chromsome_from=,is_check_processes=,agp_file="
    #     # print(sqlutil.insert_table_contact_person(type=1))
    #     # # "not null type integer"
    #     # logging.info("insert assembly method version")
    #     # print(sqlutil.insert_table_assembly_method_version())
    #     # logging.info("insert assignment")
    #     # print(sqlutil.insert_table_assignment(minimum_gap_length=100))
    #     # logging.info("insert author")
    #     # print(sqlutil.insert_table_author(type=1))
    #     # logging.info("insert genral info")
    #     # print(sqlutil.insert_table_general_info(repersent=1))
    #     # logging.info("insert new assembly")
    #     # print(sqlutil.insert_table_new_assembly(assembly_level=1,chloroplast=1,mitochondrion=1,qc=1,is_released=1))
    #     # logging.info("inset reference author")
    #     # print(sqlutil.insert_table_reference_author(type=1))
    #     # logging.info("insert reference")
    #     # print(sqlutil.insert_table_reference())
    #     # logging.info("insert submission")
    #     # print(sqlutil.insert_table_submission(is_delete=1,is_released=1,sub_status=1,flag=1))
    #     # logging.info("insert submitter")
    #     # print(sqlutil.insert_table_submitter())
    #     # logging.info("insert technology and coverage")
    #     # print(sqlutil.insert_table_technology_and_coverage(genome_coverage=100))
    # except Exception as e:
    #     logger.info("Transction Error:%s".format(e))
    #     sqlutil.rollback()
    # finally:
        
    #     sqlutil.close()


    """下面代码模拟多用户操作，开启后会同时模拟多个用户插入"""

create table if not exists batch_log ( ID integer PRIMARY KEY AUTOINCREMENT,  BATCHID text not null UNIQUE , INIT_EMAIL_TIME text not null, CURR_EMAIL_TIME text not null, PROCESS_LAST_TIME text, BIGDACCN text , BATCHID_DIR text , STATUS integer not null default 0, IDLIST text , FTP_DIR text , PROCESS_DIR text , IS_RUNNING integer not null default 0, EMAIL text not null, EXCEL_MD5 text );
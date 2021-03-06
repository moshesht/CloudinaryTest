# -*- coding: utf-8 -*-
"""
Created on Thu Aug 12 16:53:33 2021

@author: Moshe Shtadlan
"""
import inspect
import datetime
import mysql.connector
import logging
import sys
logging.basicConfig(filename='C:\mysql\log\log.txt', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(funcName)s %(message)s')
logger=logging.getLogger(__name__)

def my_handler(type, value, tb):
    logger.exception("Uncaught exception: {0}".format(str(value)))

# Install exception handler
sys.excepthook = my_handler


source_db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd  = "password123",
        database="source_db"
        )

target_db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd  = "password123",
        database="source_db"
        )


source_cursor = source_db.cursor()
target_cursor = target_db.cursor()
stg_dir = "C:/mysql/STG/"
ct = datetime.datetime.now()
#------------------------

upsert_sql = "replace INTO SOURCE_DB.source_tbl (id, s3_path, format, type, updated_at) values (%s,%s,%s,%s,%s)"

first_hour = [
    ("1",  "/a.jpg", "jpg",1,"2021-01-01T00:01"),
    ("2" , "/b.jpg", "jpg",1,"2021-01-01T00:20"),    
    ("3",  "/c.jpg", "jpg",1,"2021-01-01T00:30")    
    ]

second_hour = [
    ("1",  "/XX.jpg","jpg",1,"2021-01-01T01:21"),
    ("4" , "/d.mp4", "mp4",2,"2021-01-01T01:30"),    
    ("5",  "/e.jpg", "jpg",1,"2021-01-01T01:40")    
    ]

third_hour = [
    ("2",  "/zz.gif","gif",1,"2021-01-01T02:30"),
    ]

# -------------------------------------------

def extract_run_start() :
# extracts the  last record updated date from the last successful run   - return current run upate_at start
    logger.info (" - starting run")
    sql_extract_run_start = "Select coalesce(max(updated_at_end ), '2000-01-01T00:00') as start_Date from target_db.run_log where run_status=  'SUCCESS'"
    target_cursor.execute (sql_extract_run_start) 
    for tbl in  target_cursor:
        a= (tbl[0])        
    return (a)
    

def check_schema_evolution():
# check for changes between source table and target table structure
    sql_schema_source="SELECT COLUMN_NAME , DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'source_TBL' and table_schema = 'source_db'"
    sql_schema_target="SELECT COLUMN_NAME , DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'target_tbl' and table_schema = 'target_db'"
    source_cursor.execute (sql_schema_source) 
    target_cursor.execute (sql_schema_target) 
    source_schema= source_cursor.fetchall()
    target_schema= target_cursor.fetchall()
    
    if source_schema== target_schema:
        logger.info (" - Schema OK %s" )
    else:
        logger.error (" - Schema changed.\n source_schema: %s \n target schema: %s" %(source_schema, target_schema) )
        logger.error(" - PROGRAM ABORTED")
        quit()
    

def insert_into_run_log(a) :
# open a new run in the run log table  - return run_id     
    update_at= a
    insert_values = ( update_at,update_at, "RUN")
    sql_insert_run_log = "insert into target_db.run_log (  updated_at_start, updated_at_end , run_status ) values (%s,%s,%s)"
    target_cursor.execute (sql_insert_run_log,insert_values ) 
    target_db.commit()
    sql_extrat_run_id = ("select max(run_id) from target_db.run_log where updated_at_start = '%s' and run_status='RUN'" %update_at)
    target_cursor.execute (sql_extrat_run_id) 
    for tbl in  target_cursor:
        run_id= (tbl[0])   
    logger.info( " - run_start_update_at  =  %s  run id  = %s " %(update_at, run_id))
    return  (run_id)


def extract_source_table(date, run_id):
# extract data from source to CSV   
    sql_extract_source= ("select id, s3_path, format, type, cast(updated_at as CHAR)  from source_db.source_tbl where updated_at > '%s' INTO OUTFILE '%sb%s.csv' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'" %(date,stg_dir, run_id))
    source_cursor.execute (sql_extract_source)      
    logger.info(" - source extracted to CSV")
    return ()


def load_to_target(run_id):
# load CSV to target table  
    run_id = run_id
    target_cursor.execute("truncate table target_db.target_tbl_stg")  
    target_db.commit()  
    sql_load_stg =  ("LOAD DATA INFILE '%sb%s.csv' INTO TABLE target_db.target_tbl_stg FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'" %(stg_dir,run_id) )
    target_cursor.execute (sql_load_stg)
    target_db.commit()
    target_cursor.execute("replace into target_db.target_tbl  select * From target_db.target_tbl_stg")
    target_db.commit()
    logger.info(" - Data was loaded to target")
    return()
  

def create_delete_vector() :
# finds all missing numerators in source  and inserts them to a table  
    target_cursor.execute("truncate table target_db.delete_vector")  
    target_db.commit()
    delete_vector_sql =  "with digit as ( select 0 as d union all select 1 union all select 2 union all select 3 union all   select 4 union all select 5 union all select 6 union all    select 7 union all select 8 union all select 9 )," + " all_seq as ( select a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) + (10000 * e.d) + (100000 * f.d)+ (1000000 * g.d) as  num" + " from digit a   cross join digit b cross join digit c cross join  digit d cross join digit e  cross join digit f  cross join  digit g order by 1 desc) ," + " seq as (select num from all_seq where num <=  (select max(id) from  source_db.source_tbl ) and num<>0 )" + " select num, num as num2 from  seq a left join source_db.source_tbl b on a.num= b.id where b.id is  null "
    source_cursor.execute (delete_vector_sql)        
    delete_vector= source_cursor.fetchall()
    vector_insert_sql = "insert INTO target_db.delete_vector (id, id2) values (%s, %s)"
    target_cursor.execute("truncate table target_db.delete_vector")
    target_cursor.executemany(vector_insert_sql, delete_vector)
    target_db.commit()
    logger.info(" - delete vector created")
    return (delete_vector )


def delete_rows_From_target():
# delete rows from thetarget by ID's
      
    create_delete_vector()
    delete_sql = ("delete from target_db.target_tbl where id in (select id from target_db.delete_vector)"  )
    target_cursor.execute (delete_sql)
    target_db.commit()
    logger.info(" - delete rows finished")
    return()


def process_validation():
# comprare row count between source and target table if not identical issue an ERROR and abort
    tolerance = 0.01
    sql_val_source="select count(*) from source_db.source_tbl"
    sql_val_target="select count(*) from target_db.target_tbl"
    source_cursor.execute (sql_val_source)  
    target_cursor.execute (sql_val_target)  
    for tbl in  source_cursor :
         val_source= (tbl[0])
    for tbl in  target_cursor :
         val_target= (tbl[0])
    
    if abs(val_source - val_target)/val_source <tolerance:
        logger.info(" - process validation OK val_source = '%s' val_target = '%s'" %(val_source, val_target))
        return()
    else:
        logger.error(" - process validation failed val_source = '%s' val_target = '%s'" %(val_source, val_target))
        logger.error(" - PROGRAM ABORTED")
        quit()
        
  
def close_run(run_id):
# after a sucess load update the run log with end date and status
    sql_close_Date = ("select max(updated_at) from target_db.target_tbl")
    target_cursor.execute (sql_close_Date)  
    for tbl in  target_cursor  :
        update_at_end= (tbl[0])
    sql_update_log = ("update target_db.run_log set run_status = 'SUCCESS', updated_at_end = '%s'  where run_id= '%s'"  %(update_at_end, run_id)  )
    logger.info(" - run finished update_at_end = %s" %update_at_end)
    target_cursor.execute (sql_update_log)  
    target_db.commit()
    return()
 

# -------------------------
  
def full_process():
#Full ELT process    
    run_start_update_at= extract_run_start()
    check_schema_evolution()
    run_id =  insert_into_run_log(run_start_update_at)
    extract_source_table(run_start_update_at, run_id)
    load_to_target(run_id)
    delete_rows_From_target()
    process_validation()
    close_run(run_id)
              
# ----------------------

def main():
#run all steps of source change + Full process 

    logger.info(" - DAY ONE")       
    logger.info("-----------------------------")
    source_cursor.executemany(upsert_sql, first_hour)
    source_db.commit()
    full_process()
    logger.info(" - DAY TWO")           
    logger.info("-----------------------------")
    source_cursor.executemany(upsert_sql, second_hour)
    source_db.commit()
    full_process()
    logger.info(" - DAY THREE")                  
    logger.info("-----------------------------")
    source_cursor.executemany(upsert_sql, third_hour)
    source_cursor.execute("delete from  source_tbl where id=3")
    source_db.commit()
    full_process()    

# ----------------------      

main()


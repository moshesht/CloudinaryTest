CREATE DATABASE source_db;
CREATE DATABASE target_db;

CREATE TABLE source_db.source_tbl (id INTEGER PRIMARY KEY,s3_path NVARCHAR(1024) , format NVARCHAR(5),  type TINYINT,updated_at   TIMESTAMP  )
;
CREATE TABLE target_db.target_tbl (id INTEGER PRIMARY KEY,s3_path NVARCHAR(1024) , format NVARCHAR(5),  type TINYINT,updated_at   TIMESTAMP  )
;
CREATE TABLE target_db.target_tbl_stg (id INTEGER PRIMARY KEY,s3_path NVARCHAR(1024) , format NVARCHAR(5),  type TINYINT,updated_at   TIMESTAMP)
;
create table target_db.delete_vector  (id  INTEGER, id2 integer)
;
create table target_db.run_log (run_id integer AUTO_INCREMENT PRIMARY KEY  , run_start_date  TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at_start  TIMESTAMP ,updated_at_end TIMESTAMP ,  run_status  varchar (50)) 
;
-- ###########################

<snippet>
  <content><![CDATA[

# ${1:Poor Man's CDC - Moshe}
The project create a full process CDC from sourc table to remote target table.
The project was developed on on windows using Spider Editor , MYSQL , MYSQL workbench and git bash.

## Installation
1. Install an instance of MYSQL
2. Inastall python.
3. install mysql connector & MYSQL workbench
4. STG file folder is defined to variable stg_dir in the python acript and needs to be adjusted according to  MYSQL environment.

## Usage
1. Run Section 1 in MYSQLDB.SQL file to create databases and tables.
2. from CLI run  >> python python_cdc.py

## process description:
1. identify last run update_DT end 
2. open record in log for current run.
3. collect run id created in the log table in step 2
4. extract data from source using the date identified in step 1.
5. write the result to CSV  with run _id prefix  usong MYSQL INTO FILE command
6. ingest CSV  using  INFILE  and replace command in MYSQL
7. check for all deleted numerator in the source and write to Delete vector table
8. delete vector ID 's from target table
9. close run with SUCCESS status

** if a process fail at any point, next process that will run will know how to recover (take the right date to close the gap , delete temporaries etc). 


]]></content>
  <tabTrigger>readme</tabTrigger>
</snippet>
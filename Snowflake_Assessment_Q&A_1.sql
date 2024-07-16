--Vimalraj Chezhiyan------------------1009212

1. USE WAREHOUSE COMPUTE_WH_XL; 

2. SELECT JSON_VALUE(vehicle_data, '$.supplier_name') AS supplier_name
FROM vehicle_inventory;

3. snowsql -a   ee58380.central-india.azure (account identifier)
user: msvimalrajch
password:
use role accountadmin;
use warehouse warehouse_name;
show database;

put file://c:\temp\my_table\upload

LIST @~;  ---> FOR USER STAGES
LIST @%STAGE_NAME; ---> FOR TABLE STAGES ONLY
 
CREATE STAGE NAMED_STAGE; 
SHOW STAGES;
 
LIST @STAGE_NAME; ---> FOR NAMED STAGES ONLY 

4.CREATE WAREHOUSE XF_TUTS_WH WITH 
  WAREHOUSE_SIZE = 'X-SMALL', 
  AUTO_SUSPEND = 600, 
  AUTO_RESUME = TRUE, 
  INITIALLY_SUSPENDED = TRUE;
  
5.create or replace file_format csv_file_format
type = ‘CSV’
field_optionally_enclosed_by = ‘ """" ’
field_delimiter = ‘ ,’
Skip_header = 1; 
copy into customer
  FROM @my_stage.customer.csv
  FILE_FORMAT = (FORMAT_NAME = my_csv_format)
 ON_ERROR = 'CONTINUE/SKIP_FILE/ABORT_STATEMENT';
 
6. ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = NULL;

7.SELECT CONCAT('%', EMPLOYEE, '%') AS EMP_ from EMP_TABLE
FROM table_name;

8.SELECT car_sales:dealership::STRING AS dealership
FROM car_sales;;

9. 32 - 36 Credits 

10. SELECT  SYSTEM$PIPE_STATUS ('PIPE_NAME')
ALTER PIPE PIPE_NAME REFRESH;

11.SELECT * FROM CUSTOMER AT 
(TIMESTAMP => '<time_stamp>'::timestamp_tz);
 SELECT * FROM CUSTOMER BEFORE 
(TIMESTAMP => '<time_stamp>'::timestamp_tz); 
SELECT *
FROM CUSTOMER BEFORE
(STATEMENT => '<query_id>');

12. create or replace file_format   csv_file_format
type = ‘CSV’
field_optionally_enclosed_by = ‘ "" ’
field_delimiter = ‘ ,’
Skip_header = 1;
COPY INTO my_table
  FROM @my_stage/employee.csv
  FILE_FORMAT = (FORMAT_NAME = my_csv_format)
  ON_ERROR = 'CONTINUE'
  VALIDATION_MODE = RETURN_ERRORS; 
CREATE OR REPLACE REJECTED AS
SELECT REJECTED_RECORDS FROM TABLES (RESULT_SCAN(LAST_QUERY_ID ()));
INSERT INTO REJECTED 
SELECT REJECTED_RECORDS FROM TABLES (RESULT_SCAN(LAST_QUERY_ID ()));
SELECT * FROM REJECTED; 

13. S3 Bucket

14. SELECT DISTINCT(ID) FROM PRODUCTS
ORDER BY ID,
INSERT INTO NEW_TABLE (ID,COL_1,COL_2) SELECT DISTINCT(ID) FROM PRODUCTS
ORDER BY ID;
DROP TABLE PRODUCTS;
ALTER TABLE NEW_TABLE RENAME PRODUCTS;

15. snowsql -a ee58380.central-india.azure
user: msvimalrajch
password
 
use ingest_data;
show tables;
 
copy into @%my_stage
from my_table
file_format = (type =csv field_optionally_enclosed_by='""');
 
list @%employee
 
get @%my_stage file://c:\temp\my_table\unload

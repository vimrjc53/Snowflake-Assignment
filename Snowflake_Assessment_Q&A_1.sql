Q1. How will you use to change the warehouse for workload processing to a warehouse named ‘COMPUTE_WH_XL’?
A1. USE WAREHOUSE COMPUTE_WH_XL; 

Q2. Consider a table vehicle_inventory that stores vehicle information of all vehicles in your dealership. 
	The table has only one VARIANT column called vehicle_data which stores information in JSON format. 
	The data is given below:
	{
	“date_of_arrival”: “2021-04-28”,
	“supplier_name”: “Hillside Honda”,
	“contact_person”: {
	“name”: “Derek Larssen”,
	“phone”: “8423459854”
	},
	“vehicle”: [
	{
	“make”: “Honda”,
	“model”: “Civic”,
	“variant”: “GLX”,
	“year”: “2020”
	}
	]
	}
	What is the command to retrieve supplier_name?
A2. SELECT JSON_VALUE(vehicle_data, '$.supplier_name') AS supplier_name
	FROM vehicle_inventory;

Q3. From a terminal window, how to start SnowSQL from the command prompt ? 
	And write the steps to load the data from local folder into a Snowflake table usin three types of internal stages.
A3. snowsql -a   ee58380.central-india.azure <account identifier>

	user: msvimalrajch
	password:
	use role accountadmin;
	use warehouse warehouse_name;
	show database;

	put file://c:\temp\my_table\upload.csv @~; ---> FOR USER STAGES

	put file://c:\temp\my_table\upload.csv @%STAGE_NAME; ---> FOR TABLE STAGES ONLY

	CREATE STAGE NAMED_STAGE; 
	SHOW STAGES
	put file://c:\temp\my_table\upload.csv @STAGE_NAME; ---> FOR NAMED STAGES ONLY 

Q4. Create an X-Small warehouse named xf_tuts_wh using the CREATE WAREHOUSE command with below options 
	a) Size with x-small
	b) which can be automatically suspended after 10 mins
	c) setup how to automatically resume the warehouse
	d) Warehouse should be suspended once after created
A4.	CREATE WAREHOUSE XF_TUTS_WH WITH 
	WAREHOUSE_SIZE = 'X-SMALL', 
	AUTO_SUSPEND = 600, 
	AUTO_RESUME = TRUE, 
	INITIALLY_SUSPENDED = TRUE;

Q5. A CSV file ‘customer.csv’ consists of 1 or more records, with 1 or more fields in each record, 
	and sometimes a header record. Records and fields in each file are separated by delimiters. How will
	Load the file into snowflake table ?  
	A5.create or replace file_format csv_file_format
	type = ‘CSV’
	field_optionally_enclosed_by = ‘ """" ’
	field_delimiter = ‘ ,’
	Skip_header = 1; 

	copy into customer
	FROM @my_stage.customer.csv
	FILE_FORMAT = (FORMAT_NAME = csv_file_format)
	ON_ERROR = 'CONTINUE/SKIP_FILE/ABORT_STATEMENT';
	 
Q6. Write the commands to disable < auto-suspend > option for a virtual warehouse
A6. ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = NULL;

Q7. What is the command to concat the column named 'EMPLOYEE' between two % signs ? 
A7. SELECT CONCAT('%', EMPLOYEE, '%') AS MODIFIED_EMP from EMP_TABLE;

	Q8. You have stored the below JSON in a table named car_sales as a variant column
	{
	  "customer": [
		{
		  "address": "San Francisco, CA",
		  "name": "Joyce Ridgely",
		  "phone": "16504378889"
		}
	  ],
	  "date": "2017-04-28",
	  "dealership": "Valley View Auto Sales",
	  "salesperson": {
		"id": "55",
		"name": "Frank Beasley"
	  },
	  "vehicle": [
		{
		  "extras": [
			"ext warranty",
			"paint protection"
		  ],
		  "make": "Honda",
		  "model": "Civic",
		  "price": "20275",
		  "year": "2017"
		}
	  ]
	}
	How will you query the table to get the dealership data?
	A8.SELECT car_sales:dealership::STRING AS dealership
	FROM car_sales;;

	Q9.A medium size warehouse runs in Auto-scale mode for 3 hours with a resize from 
	Medium (4 servers per cluster) to Large (8 servers per cluster). Warehouse is resized 
	from Medium to Large at 1:30 hours, Cluster 1 runs continuously, Cluster 2 runs 
	continuously for the 2nd and 3rd hours, Cluster 3 runs for 15 minutes in the 3rd hour. 
	How many total credits will be consumed 

	A9.
			#			Clu 1	Clu 2	Clu 3
	1st hr (1 Hr) - M	  4	      0		 0
	2nd hr (30 min) - M	 4+2	  0	     0
	2nd hr (30 min) - L   0	     4+2	 0
	3rd Hr (1 hr) - L	  8 	  8	     0
	3rd Hr (15 min) - L	  0	      0	     2
	Total		         18    	14	     2---> 34

	 
	Q10. What is the command to check status of snowpipe?
	A10. SELECT  SYSTEM$PIPE_STATUS ('PIPE_NAME')
	ALTER PIPE PIPE_NAME REFRESH;

	Q11. What are the different methods of getting/accessing/querying data from Time travel.
	Assume the table name is 'CUSTOMER' and please write the command for each method.
	Q11.SELECT * FROM CUSTOMER AT 
	(TIMESTAMP => '<time_stamp>'::timestamp_tz);

	--if you are not 100% sure of the time when the update was made, 
	--you can use the BEFORE syntax and provide an approximate timestamp.
	 SELECT * FROM CUSTOMER BEFORE 
	(TIMESTAMP => '<time_stamp>'::timestamp_tz); 

	--use the timestamp and the BEFORE syntax, to travel back to how the table 
	--looked like before the delete was executed.
	SELECT *
	FROM CUSTOMER BEFORE
	(STATEMENT => '<query_id>');

Q12.If comma is defined as column delimiter in file "employee.csv" and if we get 
    extra comma in the data how to handle this scenario? 
A12.create or replace file_format csv_file_format
	type = ‘CSV’
	field_optionally_enclosed_by = ‘ "" ’
	field_delimiter = ‘ ,’
	Skip_header = 1;

	SELECT * FROM INFROMATION_SCHEMA.LOAD_HISTORY;

	COPY INTO my_table
	FROM @my_stage.employee.csv
	FILE_FORMAT = (FORMAT_NAME = csv_file_format)
	ON_ERROR = 'CONTINUE'
	VALIDATION_MODE = RETURN_ERRORS; 
	  
	CREATE OR REPLACE REJECTED AS
	SELECT REJECTED_RECORDS FROM TABLES (RESULT_SCAN(LAST_QUERY_ID ()));
	INSERT INTO REJECTED 
	SELECT REJECTED_RECORDS FROM TABLES (RESULT_SCAN(LAST_QUERY_ID ()));
	SELECT * FROM REJECTED; 

Q13.What is the command to read data directly from S3 bucket/External/Internal Stage
A13.
	---> Create external stage object
	create or replace stage ingest_data.public.ext_csv_stage
	URL = 's3://snowflake-aws-demo/snowflake/csv/'
	STORAGE_INTEGRATION = s3_int_csv
	file_format = ingest_data.public.csv_format; 
	  
	---> Create file format
	create or replace file format INGEST_DATA.public.csv_format
	type = 'csv';  
	  
	copy into customer
	FROM @ext_csv_stage.customer.csv
	FILE_FORMAT = (FORMAT_NAME = csv_file_format)
	ON_ERROR = 'SKIP_FILE';
	  
Q14.Lets assume we have table with name 'products' which contains duplicate rows. 
	How will delete the duplicate rows ?
A14.
	Method 2:
	WITH cte AS (
		SELECT 
			ID,
			ITEM,
			ROW_NUMBER() OVER(PARTITION BY ITEM ORDER BY ID) AS rank
		FROM products
	)
	DELETE FROM products
	WHERE ID IN (SELECT ID FROM cte WHERE rank > 1);

	Method 1:
	SELECT DISTINCT(ID),ITEM,QTY FROM PRODUCTS
	ORDER BY ID,
	INSERT INTO NEW_TABLE (ID,ITEM,QTY ) SELECT DISTINCT(ID) FROM PRODUCTS
	ORDER BY ID;
	DROP TABLE PRODUCTS;
	ALTER TABLE NEW_TABLE RENAME TO PRODUCTS;

Q15.How is data unloaded out of Snowflake?
A15.
	snowsql -a ee58380.central-india.azure
	user: msvimalrajch
	password:
	 
	use ingest_data;
	show tables;
	 
	copy into @%my_stage
	from my_table
	file_format = (type =csv field_optionally_enclosed_by='""');
	 
	list @%employee;
	 
	get @%my_stage file://c:\temp\my_table\unload;

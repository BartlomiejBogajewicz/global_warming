--acid + partitions
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;

--bulk processing of 1024 rows at one time
set hive.vectorized.execution.enabled = true;

--final table
 CREATE TABLE IF NOT EXISTS city_temperature
( Region String, 
  State String,
  City String,
  Month int,
  Day int,
  Year int, 
  Temp float)
PARTITIONED BY (Country String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
stored as orc 
TBLPROPERTIES('transactional'='true');	

--raw table
 CREATE TABLE IF NOT EXISTS city_temperature_temp 
( Region String, 
  Country String,
  State String,
  City String,
  Month int,
  Day int,
  Year int, 
  Temp float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

--load data
LOAD DATA INPATH "/city_temperature.csv.tmp" INTO TABLE city_temperature_temp;


INSERT INTO city_temperature PARTITION (Country)
	SELECT Region
		 , State
		 , City
		 , Month
		 , Day
		 , Year
		 , (Temp -32) * 0.56 as temp
		 , country
	FROM city_temperature_temp;
	
SELECT COUNTRY, YEAR, AVG( (avg_temp - 32) * 0.56) AS avg_temp, stddev((avg_temp - 32) * 0.56) as avg_stddev FROM city_temperature
WHERE avg_temp != -99 AND YEAR != "2020"
GROUP BY COUNTRY, YEAR	
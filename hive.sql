-- SQL script for creating table in hive
CREATE EXTERNAL TABLE word (word STRING, count INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/wordcount/output/';

-- SQL script for select required data
SELECT * FROM word ORDER BY count DESC, word ASC LIMIT 100;

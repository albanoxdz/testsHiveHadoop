CREATE EXTERNAL TABLE locations( 
    loc_id INT, 
    loc_address STRING, 
    loc_city STRING, 
    loc_zipcode STRING, 
    loc_region STRING, 
    loc_country STRING, 
    loc_lat DOUBLE, 
    loc_lon DOUBLE 
     ) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|' 
    LOCATION '${hiveconf:MY.HDFS.DIR}/locations/';

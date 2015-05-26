CREATE EXTERNAL TABLE categories( 
    cat_id INT,
    cat_name  STRING, 
    cat_description STRING 
     ) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|' 
    LOCATION '${hiveconf:MY.HDFS.DIR}/categories/'; 

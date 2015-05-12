CREATE EXTERNAL TABLE sales( 
    sale_id INT, 
    sale_cli_id  STRING, 
    sale_value  DOUBLE, 
    sale_vol     INT, 
    sale_date  TIMESTAMP, 
    sale_prod_id INT 
     ) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|' 
    LOCATION '${hiveconf:MY.HDFS.DIR}/sales/'; 

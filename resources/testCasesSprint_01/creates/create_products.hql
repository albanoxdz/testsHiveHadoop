CREATE EXTERNAL TABLE products( 
    prd_id INT, 
    prd_name  STRING, 
    prd_value  DOUBLE, 
    prd_descr  STRING, 
    prd_category_id INT 
     ) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|' 
    LOCATION '${hiveconf:MY.HDFS.DIR}/products/'; 

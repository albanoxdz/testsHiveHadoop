CREATE EXTERNAL TABLE transactions( 
    trans_id INT,     
    trans_cli_id INT, 
    trans_card_id INT, 
    trans_loc_id INT, 
    trans_date TIMESTAMP, 
    trans_value DOUBLE, 
    trans_lat DOUBLE, 
    trans_lon DOUBLE 
    ) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|' 
    LOCATION '${hiveconf:MY.HDFS.DIR}/transactions/'; 

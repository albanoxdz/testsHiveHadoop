CREATE EXTERNAL TABLE ccards( 
    card_id INT, 
    card_band   STRING, 
    card_number STRING, 
    cardv_month INT, 
    cardv_year INT, 
    card_name STRING, 
    card_lastname STRING, 
    ccard_cvv INT 
     ) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|' 
    LOCATION '${hiveconf:MY.HDFS.DIR}/cards/'; 

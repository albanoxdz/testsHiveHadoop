CREATE EXTERNAL TABLE clients( 
    cli_id INT, 
    cli_loc_id INT, 
    cli_firstname STRING, 
    cli_lastname STRING, 
    cli_phone STRING, 
    cli_cellphone STRING, 
    cli_cpf STRING, 
    cli_gender STRING, 
    cli_email STRING, 
    cli_password STRING, 
    cli_birthdate DATE, 
    cli_rg STRING, 
    cli_income STRING, 
    cli_token STRING 
    ) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY ',' 
    LOCATION '${hiveconf:MY.HDFS.DIR}/clients/';

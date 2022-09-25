TRUNCATE TABLE $login__STAGING.$table_name;
COPY $login__STAGING.$table_name ($columns)
FROM LOCAL '$filename'
DELIMITER ','
ENCLOSED BY '"'
SKIP 1
REJECTED DATA AS TABLE $login__STAGING.$table_name_rej; 

USE JD_db;
SET @@job_timeout=600000;
SET @@execute_mode='offline';
LOAD DATA INFILE '/root/project/data/JD_data/action/*.parquet' INTO TABLE action options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/root/project/data/JD_data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/root/project/data/JD_data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/root/project/data/JD_data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/root/project/data/JD_data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/root/project/data/JD_data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', header=true, mode='overwrite');

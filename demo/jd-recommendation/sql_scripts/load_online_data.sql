USE JD_db;
SET @@execute_mode='online';
LOAD DATA INFILE '/work/oneflow_demo/data/action/*.parquet' INTO TABLE action options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', mode='append');

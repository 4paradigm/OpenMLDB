create database db;
use db;
set @@execute_mode='offline';
set @@sync_job=true;
set @@job_timeout=1000000;
create table if not exists osrc(key INT, value STRING NOT NULL);
load data infile 'hive://src1' into table osrc OPTIONS(mode='overwrite');
select * from osrc;
--select * from osrc into outfile 'hive://osaved' options(mode='overwrite');
--select * from osrc into outfile 'hive://db1.osaved' options(mode='append');


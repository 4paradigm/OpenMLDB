create database if not exists simple_test;
use simple_test;
drop table if exists t1;
create table t1(c1 int, c2 string);
set @@execute_mode='online';
insert into t1 values (1, 'a'),(2,'b');
select * from t1;

/*test deploy and drop*/
deploy d1 select * from t1;
drop deployment d1;
/*async op, drop later*/
create index new_idx on t1(c2) options (ttl_type=absolute, ttl=30d);

/*empty select in offline, just to test running jobs*/
use simple_test;
set @@execute_mode='offline';
select * from t1;
set @@sync_job=true;
select * from t1;

show jobs;
show components;
show jobs from nameserver;
/*after a little, index should be added, drop is sync*/
drop index t1.new_idx;
drop table t1;
drop database simple_test;

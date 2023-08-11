create database udf_test;
use udf_test;
create table t1(c1 int, c2 string);
create function cut2(x STRING) RETURNS STRING OPTIONS (FILE='libtest_udf.so');
create aggregate function special_sum(x BIGINT) RETURNS BIGINT OPTIONS (FILE='libtest_udf.so');
create aggregate function third(x BIGINT) RETURNS BIGINT OPTIONS (FILE='libtest_udf.so', ARG_NULLABLE=true, RETURN_NULLABLE=true);
/* only test cut2 */
set @@execute_mode='offline';
select cut2(c2) from t1;
set @@execute_mode='online';
insert into t1 values (1, 'abcd'),(2,'efgh');
select cut2(c2) from t1;
drop function cut2;
drop function special_sum;
drop function third;
drop table t1;
drop database udf_test;

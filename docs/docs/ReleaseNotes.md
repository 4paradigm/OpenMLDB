# Release Notes

fesql release notes

## v0.1.1.0

时间2019-12-12 
支持window聚合函数

功能列表
* 支持了window处理函数
* 建表支持配置索引信息
* 存储引擎升级，支持segment提升并发能力
* 支持insert插入语句执行
* 简单支持database概念，包含创建

sql剧本
```
create table IF NOT EXISTS t2(
    column1 int NOT NULL,
    column2 int NOT NULL,
    column3 float NOT NULL,
    column4 bigint NOT NULL,
    column5 int NOT NULL,
    column6 string,
    index(key=column6, ts=column4)
);
insert into t2 values(1, 2, 3.3, 1000, 5, "hello");
insert into t2 values(1, 3, 4.4, 2000, 6, "world");
insert into t2 values(11, 4, 5.5, 3000, 7, "string1");
insert into t2 values(11, 5, 6.6, 4000, 8, "string2");
insert into t2 values(11, 6, 7.7, 5000, 9, "string3");
insert into t2 values(1, 2, 3.3, 1000, 5, "hello");
insert into t2 values(1, 3, 4.4, 2000, 6, "world");
insert into t2 values(11, 4, 5.5, 3000, 7, "string1");
insert into t2 values(11, 5, 6.6, 4000, 8, "string2");
insert into t2 values(11, 6, 7.7, 5000, 9, "string3");

%%fun
def test(a:i32,b:i32):i32
    c=a+b
    d=c+1
    return d
end
%%sql
SELECT column1, column2,test(column1,column5) as f1 FROM t2 limit 10;


select
sum(column1) OVER w1 as w1_col1_sum, 
sum(column2) OVER w1 as w1_col2_sum, 
sum(column3) OVER w1 as w1_col3_sum, 
sum(column4) OVER w1 as w1_col4_sum, 
sum(column5) OVER w1 as w1_col5_sum 
FROM t2 WINDOW w1 AS (PARTITION BY column6 ORDER BY column4 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) limit 100;

```

sql剧本


## v0.1.0.0

时间2019-11-20

功能列表
* 支持简单建表语句，不支持指定索引信息
* 支持简单udf定义
* 支持简单sql查询以及结合udf使用
* 支持在console上面执行sql语句

sql剧本
```sql
%%fun
def add(a:i32, b:i32):i32
    c = a + b
    return c

end
%%sql
select
    id, name, add(amt, 1) as trans_amt
from trans limit 10;
```

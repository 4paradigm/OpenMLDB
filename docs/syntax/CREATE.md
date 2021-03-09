# 建表语句


## 建立一个key索引, 用ts指定时间列

```SQL
create table test(
                     column1 int NOT NULL,
                     column2 timestamp NOT NULL,
                     column3 bigint NOT NULL,
                     column4 string NOT NULL,
                     index(key=column4, ts=column3)
);
```

## 建立多key索引

```SQL
create table test(
    column1 int NOT NULL,
    column2 timestamp NOT NULL,
    column3 bigint NOT NULL,
    column4 string NOT NULL,
    index(key=(column4, column3), ts=column2)
);
```


## 建立一个多key多版本索引

```SQL
create table test(
    column1 int NOT NULL,
    column2 timestamp NOT NULL,
    column3 int NOT NULL,
    column4 string NOT NULL,
    column5 bigint NOT NULL,
    index(key=(column4, column3), version=(column5, 2))
);
```
使用column5 作为版本号，并且最多保留两个版本

## 建立一个多key时序索引

```SQL
create table test(
    column1 int NOT NULL,
    column2 timestamp NOT NULL,
    column3 int NOT NULL,
    column4 string NOT NULL,
    column5 int NOT NULL,
    index(key=(column4, column3), ts=column2, ttl=60d)
);
```
使用column2 作为时间索引，并且数据保留60天

## 自定义partition key

```SQL
create table test(
    column1 int NOT NULL,
    column2 timestamp NOT NULL,
    column3 int NOT NULL,
    column4 string NOT NULL,
    column5 int NOT NULL,
    index(key=(column4, column3), version=(column5,2), pk=column4)
);
```
使用column4作为partition key，如果不设置partition key 默认使用索引key

## 指定replica num

```SQL
create table test(
    column1 int NOT NULL,
    column2 timestamp NOT NULL,
    column3 bigint NOT NULL,
    column4 string NOT NULL,
    index(key=column4, ts=column3)
) replicanum=2;
```
## 指定replica num, 并指定主从节点分布

```SQL
create table test(
    column1 int NOT NULL,
    column2 timestamp NOT NULL,
    column3 bigint NOT NULL,
    column4 string NOT NULL,
    index(key=column4, ts=column3)
) replicanum=2, distribution(leader="127.0.0.1:9927", follower="127.0.0.1:9926");
```

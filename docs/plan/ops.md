# 基础op抽象

为了满足sql能够批量执行和实时执行(输入一行样本计算出一行特征)两种模式,将执行op分为一下几类

* partition 分片数据，批量模式会执行分片操作，实时执行编译时会去check索引信息是否正确
* sort_partition 在单个partition内部按照某几列进行排序操作
* map 对每行数据进行行变换操作
* map_partition 迭代分片，给函数传入分片迭代器
* window 只试用在线场景，计算window最新一条记录
* slide_window 适用离线和在线场景，window多少条数据就会输出多少条记录
* filter 
* reduce
* join 多表拼接操作
* take
* union

离线和在线都共用这些op，在线分析存储索引可以如果满足op需求可以跳过部分op，比如在线存储已经按照partition key做好索引并且在sort字段做了二级索引就可以跳过partition op 和 sort op

partition key 可以设置 value, 如果设置具体value 那就只计算这个value下面window

## op解释

### partition

partition 可以再细分为hash_partition range_partition

输入为dataset
范围为dataset，此时dataset包含了一些分片信息

### sort partition

在单个分片内部进行排序
输入为dataset, dataset必须包含分片信息
输出为dataset, 此时dataset包含一些order信息

### filter 

进行过滤操作

### map

输入参数
* 迭代器 或者 表
* 函数
* 迭代次数(optional)

输入为dataset
输出为dataset

## 在线sql转换为基础op


```sql
select col1 from t1 limit 10
```

op计划

```
map(t1.it, project_fn, 10)
```

```sql
select
sum(column1) OVER w1 as w1_col1_sum,
sum(column2) OVER w1 as w1_col2_sum,
sum(column3) OVER w1 as w1_col3_sum,
sum(column4) OVER w1 as w1_col4_sum,
sum(column5) OVER w1 as w1_col5_sum
FROM t2 WINDOW w1 AS (PARTITION BY column6 ORDER BY column4 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) limit 100;
```

op计划 batch模式

```
partition(t2, column6)
    |
     \
     sort_partition(view, column4)
        |
         \
         map_partition(view)
           |
            \
             slide_window(view)
             |
              \
               take(view, 100)
```

op计划 realtime模式

```
partition(t2, column6) // 用于获取pk索引
    |
     \
     sort_partition(view, column4) // 判断二级索引
        |
         \
         map_partition(view) // skip
           |
            \
             window(view, pk) // 
             |
              \
               take(view, 100)
```


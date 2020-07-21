# 多列key 索引

index = (k1, k2, k3, (k1))

* 第一个key为多列key tuple
* 最后一个参数为自定义partition key

## sql使用场景

left_join

# 多列key 多版本索引

index = ((k1, k2, k3), version, count, (k1))

* 第一个key为多列key tuple
* 第二个为版本号
* 第三个为保留版本个数
* 第四个为可选参数，pk 代表数据做partition的key

## sql使用场景

last_join

# 多列key 时间索引

index = ((k1,k2,k3), ts, ttl, (k1))

* 第一个key为多列key tuple, 支持string,int32,int64类型
* 第二个key为时间列，支持datetime，date
* 第三个为可选参数，ttl 代表每个组合key下面数据存活时间
* 第四个为可选参数，pk 代表数据做partition的key

索引顺序默认为倒序

## 索引支持操作

* 支持全索引迭代
* 支持seek 到第一列key，然后scan一个时间范围数据

## sql层面支持的操作

这类索引只要用于时间窗口函数操作

# 纯时序索引

index = (ts, ttl, (k1, k2))

* 时间列，为系统生成时间
* ttl 为可选参数，代表数据存活时长
* 第三列为pk，用于数据分片

## 索引支持操作

# 向量索引



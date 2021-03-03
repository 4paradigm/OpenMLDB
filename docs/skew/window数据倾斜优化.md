## window数据倾斜优化
### 背景
第四范式的特征引擎平台是针对时序场景做机器学习特征。其中离线场景经常会出现数据倾斜的问题，某一个分组key，因为有大量重复的数据打到了某一分片上，导致计算过慢。
时序场景有两个特点
1. 数据会按照时间排序，所以计算的逻辑需要保证一个窗口内数据时间有序的
2. 在计算窗口的时候，有上下界，比如0到1天，0到20天，同时还有0到10条，0到1000条数据。需要保证符合上下界的数据参与到窗口的计算


### 常见技术

#### 随机key前缀

方法：在数据倾斜严重的key中，添加随机的字符串或者数字前缀，让分片更加均匀

问题：每次添加随机前缀，会出现每次分片都不一样，导致每次计算结果都不一致

#### 时间列后缀

方法：在数据倾斜严重的key中，新增一列时间列，作为联合索引分片

问题：window计算逻辑是需要把相同的key分在一个节点上，如果按照时间列再分片，会出现本来在一个节点计算的数据被打散了，破坏输出结果

### 设计方案

根据现有技术方案发现都无法完美的解决时序场景的数据倾斜问题，因为需要保证再分片后的key要跟着时间列有序，并且相同key在同一个分片或者指定分片下，而不是随机分片。

基于此背景，我们设计了分片最小计算单元window。

最小计算单元window需要满足两个定义

1. 数据完整性：window有上下界，可以根据时间划分，也可以根据条数划分
2. 数据有序性：数据需要根据时间列在单元内有序，并且在全局数据中也是全局有序

根据最小计算单元window，可以对整个数据做合理划分

1. 统计每个key的条数
2. 统计时间列的4分位点，并已知每个分位点时间值
3. 新增一列tag，并根据每个分位点的区域，分别赋值1，2，3，4
4. 为了保证数据完整性，需要在每个分位点的尾部，添加一个最小计算单元大小的数据，确保每个分位点最后一条计算的结果是有历史信息
5. 将tag字段和key字段作为联合索引，做再分片和分片内部排序，此时就将倾斜严重的一个节点，均摊到4个节点上，有效的解决数据倾斜问题
6. 同理，将分为点参数化，可以扩展为8分位点，16分位点等等。根据机器配置不同可以做灵活的调整

```
如果是4分位，并且条数最大是2条
1 1 2 3 4 4 4 4 5 5 5 5 6 6 6 6 6 6 6 6 6 7 8 8 8 9 10 10 10 12 12 12 12 15 15 15 15 16 17 17 18 18 19 20 21 22 23 24
2 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
3                     2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2
4                                             3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 
5                                                                                    4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4

第一行是原始数据并且已经有序
根据4分位和统计的信息，根据最小计算单元的上下界给原始数据打tag
第二行，第一分位打tag 1
第三行，第二分位打tag 2
第四行，第三分位打tag 3
第五行，第四分位打tag 4
```



### 案例

```
主表
+-------------+----------+-----+------+---+------+
|employee_name|department|state|salary|age| bonus|
+-------------+----------+-----+------+---+------+
|        James|     Sales|   NY| 90000| 34| 10000|
|        James|     Sales|   NY| 86000| 56| 20000|
|        James|     Sales|   CA| 81000| 30| 30000|
|        James|   Finance|   CA| 90000| 24| 40000|
|        James|   Finance|   CA| 99000| 40| 50000|
|        James|   Finance|   NY| 83000| 36| 60000|
|        James|   Finance|   NY| 83000| 36| 70000|
|        James|   Finance|   NY| 83000| 36| 80000|
|        James|   Finance|   NY| 83000| 36| 90000|
|        James|   Finance|   NY| 83000| 36|100000|
|          Jen|   Finance|   NY| 79000| 53| 15000|
|          Jen| Marketing|   CA| 80000| 25| 18000|
|          Jen| Marketing|   NY| 91000| 50| 21000|
+-------------+----------+-----+------+---+------+
 
 
分位分析sql
select
employee_name,
department,
count(employee_name,department) as key_cnt,
min(bonus) as min_bonus,
max(bonus) as max_bonus,
mean(bonus) as mean_bonus,
sum(bonus) as sum_bonus,
percentile_approx(bonus, 0.0) as percentile_0,
percentile_approx(bonus, 0.25) as percentile_1,
percentile_approx(bonus, 0.5) as percentile_2,
percentile_approx(bonus, 0.75) as percentile_3,
percentile_approx(bonus, 1) as percentile_4
from
main_table
group by employee_name , department;
 
四分位
+-------------+----------+---------+-----------+----------+------------+------------+------------+------------+------------+----------+----------+--------------------+
|employee_name|department|total_sum|order_bonus|avg(bonus)|percentile_0|percentile_1|percentile_2|percentile_3|percentile_4|max(bonus)|min(bonus)|count(employee_name)|
+-------------+----------+---------+-----------+----------+------------+------------+------------+------------+------------+----------+----------+--------------------+
|          Jen|   Finance|    15000|          1|   15000.0|       15000|       15000|       15000|       15000|       15000|     15000|     15000|                   1|
|        James|     Sales|    60000|          3|   20000.0|       10000|       10000|       20000|       30000|       30000|     30000|     10000|                   3|
|          Jen| Marketing|    39000|          2|   19500.0|       18000|       18000|       18000|       21000|       21000|     21000|     18000|                   2|
|        James|   Finance|   490000|          7|   70000.0|       40000|       50000|       70000|       90000|      100000|    100000|     40000|                   7|
+-------------+----------+---------+-----------+----------+------------+------------+------------+------------+------------+----------+----------+--------------------+
 
 
标签sql
select
main_table.employee_name,main_table.department,main_table.state,main_table.salary,main_table.age,main_table.bonus,
case
when info_table.key_cnt < 4 then 1
when main_table.bonus <= percentile_0 then 4
when main_table.bonus > percentile_0 and main_table.bonus <= percentile_1 then 4
when main_table.bonus > percentile_1 and main_table.bonus <= percentile_2 then 3
when main_table.bonus > percentile_2 and main_table.bonus <= percentile_3 then 2
when main_table.bonus > percentile_3 and main_table.bonus <= percentile_4 then 1
end as tag_wzx
,
case
when info_table.key_cnt < 4 then 1
when main_table.bonus <= percentile_0 then 4
when main_table.bonus > percentile_0 and main_table.bonus <= percentile_1 then 4
when main_table.bonus > percentile_1 and main_table.bonus <= percentile_2 then 3
when main_table.bonus > percentile_2 and main_table.bonus <= percentile_3 then 2
when main_table.bonus > percentile_3 and main_table.bonus <= percentile_4 then 1
end as position_wzx
from main_table left join info_table on main_table.employee_name = info_table.employee_name and main_table.department = info_table.department;
 
 
 
打标签
+-------------+----------+------+---+
|employee_name|department| bonus|tag|
+-------------+----------+------+---+
|        James|     Sales| 10000|  1|
|        James|     Sales| 20000|  2|
|        James|     Sales| 30000|  3|
|        James|   Finance| 40000|  1|
|        James|   Finance| 50000|  1|
|        James|   Finance| 60000|  2|
|        James|   Finance| 70000|  2|
|        James|   Finance| 80000|  3|
|        James|   Finance| 90000|  3|
|        James|   Finance|100000|  4|
|          Jen|   Finance| 15000|  1|
|          Jen| Marketing| 18000|  1|
|          Jen| Marketing| 21000|  3|
+-------------+----------+------+---+
 
 
数据扩容
 
+-------------+----------+-----+------+---+------+------+-------+
|employee_name|department|state|salary|age| bonus| bonus|tag_wzx|
+-------------+----------+-----+------+---+------+------+-------+
|        James|     Sales|   NY| 90000| 34| 10000| 10000|      1|
|        James|     Sales|   NY| 86000| 56| 20000| 20000|      2|
|        James|     Sales|   NY| 86000| 56| 20000| 20000|      2|
|        James|     Sales|   CA| 81000| 30| 30000| 30000|      3|
|        James|     Sales|   CA| 81000| 30| 30000| 30000|      3|
|        James|     Sales|   CA| 81000| 30| 30000| 30000|      3|
|        James|   Finance|   CA| 90000| 24| 40000| 40000|      1|
|        James|   Finance|   CA| 99000| 40| 50000| 50000|      1|
|        James|   Finance|   NY| 83000| 36| 60000| 60000|      2|
|        James|   Finance|   NY| 83000| 36| 60000| 60000|      2|
|        James|   Finance|   NY| 83000| 36| 70000| 70000|      2|
|        James|   Finance|   NY| 83000| 36| 70000| 70000|      2|
|        James|   Finance|   NY| 83000| 36| 80000| 80000|      3|
|        James|   Finance|   NY| 83000| 36| 80000| 80000|      3|
|        James|   Finance|   NY| 83000| 36| 80000| 80000|      3|
|        James|   Finance|   NY| 83000| 36| 90000| 90000|      3|
|        James|   Finance|   NY| 83000| 36| 90000| 90000|      3|
|        James|   Finance|   NY| 83000| 36| 90000| 90000|      3|
|        James|   Finance|   NY| 83000| 36|100000|100000|      4|
|        James|   Finance|   NY| 83000| 36|100000|100000|      4|
|        James|   Finance|   NY| 83000| 36|100000|100000|      4|
|        James|   Finance|   NY| 83000| 36|100000|100000|      4|
|          Jen|   Finance|   NY| 79000| 53| 15000| 15000|      1|
|          Jen| Marketing|   CA| 80000| 25| 18000| 18000|      1|
|          Jen| Marketing|   NY| 91000| 50| 21000| 21000|      3|
|          Jen| Marketing|   NY| 91000| 50| 21000| 21000|      3|
|          Jen| Marketing|   NY| 91000| 50| 21000| 21000|      3|
+-------------+----------+-----+------+---+------+------+-------+
```

### 性能

针对公司内部的场景

| 场景     | 开启优化      | 关闭优化   |
| -------- | ------------- | ---------- |
| 风电场景 | 11min51sec    | 22min30sec |
| 秒车场景 | 46mins, 32sec | 跑失败     |



### 使用方法

```
spark.fesql.mode = skew 开启数据倾斜优化模式
spark.fesql.skew.watershed = 10000 只针对10000条以上的数据做倾斜优化，以下的跳过
spark.fesql.skew.ratio = 0.5 某个key占总数据一半以上的做数据倾斜优化，以下的跳过
spark.fesql.skew.level = 2 优化等级，2的n次方。level = 2会按4分位点划分，level = 3会按8分位点划分
```


# 物理执行计划节点

action节点列表

|#|节点|是否block|是否lazy|描述| 
|---|---|---|---|---|
|1|group_by|是|是|分片处理上层可以对应group_by 和 window partition by, 可以支持分组表达式 |
|2|group_foreach|否|是|迭代分片 |
|3|sort_by|是|否|对数据进行排序处理 , 对应order by相关操作, 可以支持compare 函数|
|4|window_foreach|否|是|迭代每个window内部数据, 可以支持过滤表达式|
|5|table_foreach|否|否|迭代table内部数据, 可以支持过滤表达式|
|6|project|否|否|进行行变化操作|
|7|join|否|是|进行表之间的拼接操作|
|8|union|否|是|进行表合并操作|


## partition_by 

对数据做分片

参数列表
* table 
* column列表或者表达式

## partition_foreach 

这个是结合 partition_by一起使用的, 这个物理节点可以支持聚合函数， 和 滑动窗口节点

## sort_by 

对数据进行排序

参数列表
* table 
* column列表或者表达式
* ASC or DESC



## left_join + window



```
select sum(w.col5) from t1 join t1.col3 = t2.col4 
window w AS (PARTITION BY col1 ORDER BY col2)
```

col1,col2,col3 属于t1, col4,col5属于t2

逻辑计划

```
\
 \__t1 join t2 on t1.col1 = t2.col2
       |
        \
         \__ partition by col1 
               |
                \
                 \__ sort by col2
                     |
                      \
                       \__ window_agg
                             |
                              \
                               \__ project sum(col5)
```

物理计划

假设 t1已经建立了 col1,col2的联合索引， t2 建立col4的索引

```
                                project_fn
                                      |
                                      /
    project_fn              join_fn__/
        |                     |
         \                   /
          \_window_foreach__/
               |
                \
                 \
                 partition_foreach
```



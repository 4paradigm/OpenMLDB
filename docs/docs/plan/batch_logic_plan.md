# fesql batch logical plan

## 概念

* datasource 输入数据包含schema,文件类型,文件url
* datasink 
* dataset
* map 对dataset进行map函数处理
* partition 对dataset进行partition操作
* mapPartition 
* sortInPartition
* task 执行最小单元
* tasklet task执行节点
* datasetlet dataset 数据存储节点

## 简单select处理

```sql

select log(col1) from t1
```

执行计划为

```
datasource 
    |
     \
     dataset t1
       |
        \ 
         select(t1, col1)
           |
            \
            map log(co1)
              |
               \
              dataset tmp
                |
                 \
                datasink
```

物理执行计划

```
   task{
       it = create_iterator(t1)
       map(it, log)
   }
```

### 执行计划任务列表

[boost dag](https://www.boost.org/doc/libs/1_72_0/libs/graph/example/undirected_adjacency_list.cpp)

### 执行计划优化


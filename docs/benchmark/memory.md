# FeSQL Memory Benchmark 

schema1  
字段数3, 1个string字段(长度为10), 1个int64字段, 1个double字段  

scheam2  
字段数30, 10个string字段(长度为10), 10个int64字段, 10个double字段  

场景为100w的pk, 每个pk下10条时序数据

内存占用

|schema   |原始大小 |fesql |rtidb |memsql |voltdb |
| ------- | ------- | ---- | ---- | ----- | ----- |
|schema1  |247M     |1.0G  |1.2G  |995M   |1.5G   |
|schema2  |2.42G    |3.9G  |4.1G  |5.6G   |6.3G   |



时序存储引擎

一个表会分多个分片, 分片是副本迁移的最小单位  
分片内部为了减少写竞争会分多个slot, 每个slot指向有多个index组成的数组, 每个index指向一个segment  
整体结构如下图所示:   
![](/image/table.png)

segment是内部存储的基本单位. 最外层是一个跳表, 跳表的key是每个index的key, value指向一个list. 如果一个key下的数据小于某个配置的值(默认为400), list的实现是一个数组. 如果条数大于配置值list就会转化成一个单链表. 
![](/image/segment.png)

插入数据时会把整条数据encode成一个value, 然后根据每个index的值插入到不同的分片里的segment中. value会指向同一个地址
![](/image/value.png)

# 多索引设计

* 每个tablet保存当前表的拓扑状态
* 同一个tablet里多个index共享同一个value

## 插入数据

### client端
* client对待插入的数据编码  
* 依据第一个index计算需要插入的分片id
* 查询该分片id leader所在的tablet节点
* 发插入请求到该节点

### server端

#### 服务线程
* 申请内存保存value信息
* 计算每个index对应的pid
* 查询当前tablet节点该tid对应分片信息, 把数据插入到对应的分片里, 数据指向申请的value地址
* 将offset加1然后写入每个分片的binlog
* 返回写请求  

#### replica线程
* 读取binlog, 判断第一个index分片id和当前pid是否一样, 如果不一样则跳过.
* 解析出index信息, 计算每个index对应的pid
* 查询每个pid所在的tablet, 向这些tablet同步这条数据

### 服务线程处理AppendEntry请求
* 申请内存保存value
* 计算每个index对应的pid
* 查询当前tablet节点该tid对应分片信息, 把数据插入到对应的分片里, 数据指向申请的value地址
* 写入数据到各分片binlog
* 返回AppendEntry请求

## 增加index

* nameserver向该表所在的所有tablet发送增加index请求
* tablet给每个分片新增index存储结构, 如果该分片是leader分片, 创建完结构后记录offset
* leader分片启动后台线程读取snapshot和binlog将数据插入到新增的index结构中, 直到offset达到记录的offset
* leader分片将增加index时的offset传给follower节点, follower节点启动后台线程加载snapshot和binlog将数据插入到新增的index结构中, 直到offset达到记录的offset

数据加载流程  
1 解析数据, 计算新增index所在分片  
2 如果该分片在当前节点, 依据第一个index的key查出value, 将这条数据插入到新增的index中, 数据指向查出来的value. 将offset置0写binlog  
3 查出该分片所在的其他tablet, 发送AppendEntry同步请求到对应的tablet上  
4 远端tablet收到AppendEntry请求, 遍历这条数据所有index, 找到在当前tablet上存在的分片查出value, 如果不存在分片, 申请内存存放value. 将数据插入到新增index中, 地址指向对应的value. 将offset置0写binlog  

## 新增副本
* 新增分片结构
* 更新各tablet拓扑
* 记录各分片第一次同步的offset
* leader分片传snapshot到新增的分片节点
* load snapshot, 和leader同步binlog. 如果binlog里offset大于记录的首次同步offset则忽略

## 删除分片副本
直接删除指定的分片副本  


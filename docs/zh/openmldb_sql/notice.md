# 执行SQL命令需要注意的点

|  SQL 命令      |       注意点                                   |
| ------------- | --------------------------------------------- |
| CREATE TABLE  | 1. 在建表语句中如果没有指定索引，默认会自动创建一个absolute 0的索引。这个索引下的数据永不过期，可能会占用大量内存 <br> 2. 磁盘表absandlat和absorlat类型没有过期删除
| DROP TABLE    | 1. 删除表默认是异步操作，执行完成后，异步删除表中的数据 <br> 2. 如果有分片在做snapshot, 会删除失败。可能存在部分分片删除部分没有删除的情况 <br> 3. 默认会把数据目录放到recycle下。recycle_bin_enabled参数可以配置是否要放到recycle, 默认是开启的 <br> 4. 由于内存碎片问题，释放的内存不一定完全释放给操作系统
| INSERT        | 如果返回失败，可能有一部分数据已经插入进去
| DELETE        | 1. 删除的数据不会立马从内存中物理删除，需要等一个gc_interval <br> 2. 如果设置了长窗口，不会更新预聚合表里的数据
| CREATE INDEX  | 1. 创建索引是一个异步操作，如果表里有数据需要等一段时间desc才能显示出来 <br> 2. 在创建索引的过程中如果有写操作，那么可能会有部分新写入的数据在新加的索引上查询不出来 <br> 3. 磁盘表不支持创建索引
| DROP INDEX    | 1. 删除一个索引之后，如果要再重新创建相同的索引需要等两个gc_interval <br> 2. 删除索引后内存的数据不会立马删除，需要等两个gc_interval <br> 3. 磁盘表不支持删除索引
| DEPLOY        | 1. deploy SQL时可能会修改相关表的TTL，deploy前导入的数据可能新TTL生效前被淘汰 <br> 2. 在deployment关联的表中，如果有磁盘表需要添加索引，那么部署会失败，可能有部分索引已经添加成功
| DROP DEPLOYMENT | 1. 不会清理自动创建的索引 <br> 2. 如果指定了长窗口，删除deployment不会清理预聚合表
| DROP FUNCTION | 如果有正在执行的deployment用到此函数，可能会执行错误或者程序崩溃
| SHOW COMPONENTS | 1. 结果不显示API Server <br> 2. 结果不显示TaskManager standby
| SHOW JOBS     | 1. show jobs默认显示TaskManager的job。`show jobs from nameserver`显示NameServer的jobs。`show jobs from taskmanager`显示TaskManager的jobs <br> 2. NameServer重启后，没有恢复和展示已完成和已取消的job
| SHOW JOB      | 只能显示TaskManager里的job, 不支持显示NameServer里的job
| STOP JOB      | 只能停止TaskManager里的job, 不支持停止NameServer里的job
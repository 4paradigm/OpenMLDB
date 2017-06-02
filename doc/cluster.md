# rtidb cluster design

rtidb集群设计问题，rtidb要做的事情
* 管理分片，分片主节点选择
* 故障迁移

不做的事情
* 心跳管理
* 选主管理

以上事情都通过etcd／zk完成

client 只和etcd通讯去获取相关元数据，不和master通讯



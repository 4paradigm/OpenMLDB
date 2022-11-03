# OpenMLDB运维工具

## 概述

为了方便用户更方便的使用OpenMLDB，我们提供了OpenMLDB运维工具，主要包括以下功能:
- 一键数据恢复
- 扩容后自动分片均衡
- 将缩容节点的分片迁移到其他机器上

## 使用

```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=scaleout
```

### 参数说明
- --openmldb_bin_path 指定openmldb二进制文件路径
- --zk_cluster 指定openmldb zookeeper地址
- --zk_root_path 指定openmldb zookeeper路径
- --cmd 指定执行的操作。目前支持的操作有`recoverdata`, `scaleout`, `scalein` 分别表示数据恢复，扩容分片均衡，缩容分片转移
- --endpoints 指定节点，只有cmd为`scalein`时才有效，指定分片迁出的节点，如果有多个节点以逗号分隔

**注**:
- 要求python3.6及以上版本
- 脚本在OpenMLDB部署包里的tools目录，如果要在其他节点执行，需要拷贝tools目录和openmldb二进制文件
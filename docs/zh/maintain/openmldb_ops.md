# OpenMLDB运维工具

## 概述

为了用户更方便的维护OpenMLDB，我们提供了OpenMLDB运维工具，主要包括以下功能:
- 一键数据恢复：`recoverdata`
- 自动分片均衡: `scaleout`
- 将缩容节点的分片迁移到其他机器上: `scaleint`
- 升级tablet前处理和后处理：`pre-upgrade`和`post-upgrade`
- 查询操作状态：`showopstatus`
- 查询表状态：`showtablestatus`

## 使用
### 通用参数说明
- --openmldb_bin_path 指定openmldb二进制文件路径
- --zk_cluster 指定openmldb zookeeper地址
- --zk_root_path 指定openmldb zookeeper路径
- --cmd 指定执行的操作。比如`recoverdata`

### 操作说明

| 操作                | 功能                                                                                                                                | 额外参数                                                                                                |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `recoverdata`     | 恢复所有表中数据                                                                                                                          | -                                                                                                   |
| `scaleout`        | 自动分片均衡                                                                                                                            | -                                                                                                   |
| `scalein`         | 将缩容节点的分片迁移到其他机器上，如果剩余tablet的个数小于表的最大副本数会执行失败                                                                                      | --endpoints: 指定分片迁出的节点，如果有多个节点以逗号分隔                                                                 |
| `pre-upgrade`     | 升级tablet前预处理，会把该tablet上的leader分片进行转移，降低对在线任务的影响。如果leader分片只有一个副本，会自动在其他tablet上为该分片添加副本，`post-upgrade`的时候再进行删除。转移信息会记录在`statfile`中 | --endpoints: 指定升级的节点，只允许一个节点 <br/> --statfile：保存leader分片转移信息的临时文件，默认为`.stat`                        |
| `post-upgrade`    | 升级tablet后处理，会从`statfile`读取`pre-upgrade`转移的leader分片，并将其恢复成升级前的状态                                                                   | --endpoints: 指定升级的节点，只允许一个节点 <br/> --statfile：保存leader分片转移信息的临时文件，默认为`.stat`                                                                       |
| `showopstatus`    | 查询操作状态                                                                                                                            | --filter: 过滤某种状态，只显示该状态下的操作，状态取值为 `kInited`, `kDoing`, `kDone`, `kFailed`, `kCanceled`。默认为空，所有状态都展示 |
| `showtablestatus` | 查询表的状态                                                                                                                            | --filter: 过滤匹配的数据库，匹配规则和`LIKE`一致，默认为`'%'`，展示所有的数据库                                                  |

**使用示例**
```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=scaleout
```

### 环境要求
- 要求python3.6及以上版本
- 脚本在OpenMLDB部署包里的tools目录，如果要在其他节点执行，需要拷贝tools目录和openmldb二进制文件
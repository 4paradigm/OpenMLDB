# 诊断工具

## 概述

为了方便排查用户环境中的常见问题，OpenMLDB提供了诊断工具。主要有以下功能:
- 版本校验
- 配置文件检查
- 日志提取
- 执行测试SQL

## 使用

1. 下载诊断工具包
```bash
    pip install openmldb-tool
```

2. 准备环境yaml配置文件

单机版yaml
```yaml
mode: standalone
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/openmldb
tablet:
  -
    endpoint: 127.0.0.1:9527
    path: /work/openmldb
```

集群版yaml
```yaml
mode: cluster
zookeeper:
  zk_cluster: 127.0.0.1:2181
  zk_root_path: /openmldb
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/ns1
tablet:
  -
    endpoint: 127.0.0.1:9527
    path: /work/tablet1
  -
    endpoint: 127.0.0.1:9528
    path: /work/tablet2
taskmanager:
  -
    endpoint: 127.0.0.1:9902
    path: /work/taskmanager1
```

3. 添加机器互信

    由于诊断工具需要到部署节点上拉取文件，所以需要添加机器互信免密。设置方法参考[这里](https://www.itzgeek.com/how-tos/linux/centos-how-tos/ssh-passwordless-login-centos-7-rhel-7.html)

4. 执行诊断工具命令
```bash
openmldb_tool --dist_conf=/tmp/standalone_dist.yml
```
诊断工具主要参数如下:

- --dist_conf OpenMLDB节点分布的配置文件
- --data_dir 数据存放路径。会把远端的配置文件和日志等放在这个目录里，默认为/tmp/diagnose_tool_data
- --check 检查项，默认为ALL即检查所有。还可以单独配置为CONF/LOG/SQL/VERSION，分别检查配置文件、日志、执行SQL、版本
- --exclude 不检查其中某一项。只有check设置为ALL才会生效。可以配置为CONF/LOG/SQL/VERSION
- --log_level 设置日志级别，默认为info。可以设置为debug/warn/info
- --log_dir 设置结果输出路径，默认为标准输出
- --env 如果用start-all.sh启动的集群，需要指定为onebox, 其他情况不需要指定

例如指定只检查配置文件，并且结果输出到当前目录下
```
openmldb_tool --dist_conf=/tmp/cluster_dist.yml --check=conf --log_dir=./
```

**注**: 如果是单机版，诊断工具必须在单机版部署节点上执行  

可使用`openmldb_tool --helpfull`查看所有配置项。例如，`--sdk_log`可以打印sdk的日志（zk，glog），可用于调试。
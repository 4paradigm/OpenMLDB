# 在线数据导出工具

在线数据导出工具位于[src/tools](https://github.com/4paradigm/OpenMLDB/tree/main/src/tools)。支持导出远程机器中单机或集群模式的数据。

## 1. 导出工具工具安装

Unix Executable文件生成：src目录下使用make。

## 2. 导数工具使用

### 2.1 命令参数

所有的配置项如下，星号表示必填项。

```
Usage: ./data_exporter [--delimiter=<delimiter>] --db_name=<dbName> 
                     [--user_name=<userName>] --table_name=<tableName>
                     --config_path=<configPath>
      
*     --db_name=<dbName>          openmldb database name
*     --table_name=<tableName>    openmldb table name of the selected database
*     --config_path=<configPath>  absolute or relative path of the config file
      --delimiter=<delimiter>     delimiter for the output csv, default is ','
      --user_name=<userName>      user name of the remote machine
```

### 2.2 重要参数配置说明

重要配置的项目说明：

- `--db_name=<dbName>`: 库名。库名必须是存在的，如果不存在则报错：数据库不存在。
- `--table_name=<tableName>`: 表名。表名必须是存在的，如果不存在则报错：表不存在。
- `--config_path=<configPath]`: OpenMLDB远程机器的配置文件，文件格式是yaml。

### 2.3 配置文件用例：

     mode: cluster
     zookeeper:
         zk_cluster: 172.17.0.2:2181
         zk_root_path: /openmldb
     nameserver:
     - 
         endpoint: 172.17.0.2:6527
         path: /work/ns1
     - 
         endpoint: 172.17.0.2:6528
         path: /work/ns2
     tablet:
     - 
         endpoint: 172.17.0.2:10921
         path: /work/openmldb
     - 
         endpoint: 172.17.0.2:10922
         path: /work/openmldb

## 3. 错误处理

如果数据导出失败，可以根据Glog的错误信息来判断错误原因。

# 在线数据导入工具

在线数据导入工具位于[java/openmldb-import](https://github.com/4paradigm/OpenMLDB/tree/main/java/openmldb-import)。支持两种导入方式，insert和bulk load。

- insert方式：适用于所有场景的数据导入，目前还在优化中，暂不能使用。
- bulk load方式：适用于空表并且导入期间没有读写的场景。有更高的导入效率，但目前只支持本地多csv文件的导入。暂不支持Map-Reduce模式。

## 1. 导数工具安装

工具jar包生成：openmldb-import目录中可以使用mvn打包出opemmldb-import.jar。

```bash
> cd java/openmldb-import
> mvn package
```

`mvn package -Dmaven.test.skip=true` 跳过测试。

## 2. 导数工具使用

### 2.1 命令参数

--help可以展示出所有的配置项，星号表示必填项。

```bash
> java -jar openmldb-import-1.0-SNAPSHOT.jar --help
```

```
Usage: Data Importer [-fhV] [--create_ddl=<createDDL>] --db=<dbName>
                     [--importer_mode=<mode>]
                     [--rpc_size_limit=<rpcDataSizeLimit>] --table=<tableName>
                     -z=<zkCluster> --zk_root_path=<zkRootPath> --files=<files>
                     [,<files>...] [--files=<files>[,<files>...]]...
insert/bulk load data(csv) to openmldb
      --create_ddl=<createDDL>
                            if table is not exists or force_recreate_table is
                              true, provide the create table sql
*     --db=<dbName>         openmldb database
  -f, --force_recreate_table
                            if true, we will drop the table first
*     --files=<files>[,<files>...]
                            sources files, local or hdfs path
  -h, --help                Show this help message and exit.
      --importer_mode=<mode>
                            mode: Insert, BulkLoad. Case insensitive.
      --rpc_read_timeout=<rpcReadTimeout>
                            rpc read timeout(ms)
      --rpc_size_limit=<rpcDataSizeLimit>
                            should >= 33554432
      --rpc_write_timeout=<rpcWriteTimeout>
                            rpc write timeout(ms)
*     --table=<tableName>   openmldb table
  -V, --version             Print version information and exit.
* -z, --zk_cluster=<zkCluster>
                            zookeeper cluster address of openmldb
*     --zk_root_path=<zkRootPath>
                            zookeeper root path of openmldb
```

### 2.2 重要参数配置说明

重要配置的项目说明：

- `--importer_mode=<mode>`: 导入模式，支持insert和bulkload两种方式。默认配置为bulkload.

- `--zk_cluster=<zkCluster>`和`--zk_root_path=<zkRootPath>`: 集群版OpenMLDB的ZK地址和路径。
- `--db=<dbName>`: 库名。库名可以是不存在的，importer可以帮助创建。
- `--table=<tableName>`: 表名。表名可以是不存在的，importer可以帮助创建，需配置`--create_ddl`。但请注意，如果导入到已存在的表，需要表内数据为空，否则将会极大影响导入效率。

- `--files=<files>[,<files>...]`: 导入源文件。目前源文件只支持csv格式的本地文件，并且csv文件必须有header，文件的列名和表的列名必须一致，顺序可以不一样。

## 3. 大规模的数据导入

针对大规模的数据导入，我们建议使用bulk load的导入方式，可以快速进行导入操作。`--importer_mode=bulkload`

但请注意:warning:，导入前，**表数据必须为空**，导入期间**不可以**有任何对该表的读写。本文主要介绍OpenMLDB数据导入工具在bulk load方式下的使用方法。

### 配置调整

由于目前import只是单机模式，大数据导入可能较大地消耗资源，一些操作会变得很耗时。

如果导入中出现oom，请调大-Xmx。如果出现rpc send失败，调大rpc_write_timeout。如果rpc回复timeout，也适当调大rpc_read_timeout。

### 错误处理

只要importer端没有打印输出`bulk load succeed`，importer中途退出或者打印输出failed，都可以视为导入失败。

由于导入失败的状态可能很复杂，所以正确的错误处理方式是**drop导入失败的表，重建表再进行导入**。

删表重建的操作，可以通过配置-f，并--create_ddl提供建表语句，让importer帮忙完成这部分前置工作，然后接着进行新的导入。


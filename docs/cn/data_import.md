# DataImport
数据导入工具位于/java/openmldb-import。预计支持两种导入方式，insert和bulk load。insert模式，适用于所有场景的数据导入，目前还在优化中；bulk load模式只适合在空表并且导入期间没有读写的场景，有更高的导入效率，可以投入使用，但目前只支持本地多csv文件的导入。

## Bulk Load

针对大规模的数据导入，我们支持bulk load的导入方式，可以快速进行导入操作。但请注意，导入前，**表数据必须为空**，导入期间**不可以**有任何对该表的读写。

### Bulk Load流程

bulk load可以抽象为两个组件的协同工作，一个是importer端，即import工具自身；一个是loader端，即openmldb的tablet server。

通常的插入流程是importer只需将数据发送到loader端（仅限主副本），loader端解析并插入（包括“定位到segment”和“插入segment的两级跳表内“），然后写binlog（用于从副本的数据同步）。bulk load本质是将数据插入的过程提前，由importer端组织好数据结构（生成所有segment），然后直接发送给loader，让loader可以在线性时间内重建出所有segment，完成整个数据导入流程。

我们将数据本身命名为data region（binlog暂定属于data region），将多个segment结构命名为index region。importer在扫描源数据时就会分批发送data region的part。当所有数据都被处理完毕时，就可以发送index region（由于loader端的rpc大小限制，同样可能分批发送）。最后一个index region part发送完成后，整个导入流程就成功了。

### Bulk Load使用

工具jar包生成：openmldb-import目录中可以使用mvn打包出opemmldb-import.jar。

使用方法：

```
> java -jar openmldb-import.jar --help
Usage: Data Importer [-fhV] [--create_ddl=<createDDL>] --db=<dbName>
                     [--importer_mode=<mode>]
                     [--rpc_size_limit=<rpcDataSizeLimit>] --table=<tableName>
                     -z=<zkCluster> --zk_root_path=<zkRootPath> --files=<files>
                     [,<files>...] [--files=<files>[,<files>...]]...
insert/bulk load data(csv) to openmldb
      --create_ddl=<createDDL>
                            if force_recreate_table is true, provide the create
                              table sql
*     --db=<dbName>         openmldb database
  -f, --force_recreate_table
                            if true, we will drop the table first
*     --files=<files>[,<files>...]
                            sources files, local or hdfs path
  -h, --help                Show this help message and exit.
      --importer_mode=<mode>
                            mode: Insert, BulkLoad. Case insensitive.
      --rpc_size_limit=<rpcDataSizeLimit>
                            should >= 33554432
*     --table=<tableName>   openmldb table
  -V, --version             Print version information and exit.
* -z, --zk_cluster=<zkCluster>
                            zookeeper cluster address of openmldb
*     --zk_root_path=<zkRootPath>
                            zookeeper root path of openmldb
```

--help可以展示出所有的配置项，星号表示必填项。

导入必须配置openmldb地址，库名表名，以及导入源文件。目前源文件只支持csv格式的本地文件，并且csv文件必须有header，文件的列名和表的列名必须一致，顺序可以不一样。

库名表名可以是不存在的，importer可以帮助创建。但请注意，如果导入到已存在的表，需要表内数据为空，否则将会极大影响导入效率。

#### 错误处理

只要importer端没有打印输出`bulk load succeed`，importer中途退出或者打印输出failed，都可以视为导入失败。

由于导入失败的状态可能很复杂，所以正确的错误处理方式是**drop导入失败的表，重建表再进行导入**。

删表重建的操作，可以通过配置-f，并--create_ddl提供建表语句，让importer帮忙完成这部分前置工作，然后接着进行新的导入。


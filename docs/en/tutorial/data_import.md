# Data Import
Data import tool locates in folder `/java/openml-import`. There are two ways to import data: `insert` mode and `bulk load` mode.

`insert` mode is suitable for almost every situation, and we are keeping optimizing it.

`bulk load` mode is only suitable for empty table loading with no reading and writing before the loading process stopped. 

The `bulk load` mode is more efficient and is available for multiple local files with csv format. 

## Bulk Load

`bulk load` can be used for large-scale data importing, and it's fast.
But be careful, you have to make sure the target table empty before the data is loaded, and don't do some read or write operation during data importing.

### Bulk Load Process

`bulk load` is driven by two components. The first one is the importer, it's the import tool itself. The other one is the loader, it's the tablet server of openmldb.

Usually, there are three steps for importing data.
Firstly, the importer sends the primary data to the loader.
Secondly, the loader parses the data and does insert operation, which includes locating the segment and inserting data into two level skip list of the segment.
Thirdly, binlog is written for synchronizing replica data. 

`bulk load` makes the data inserting process executing first. Then the importer generates all the segments, and sends to the loader directly. The loader can rebuild all the segments in linear time and complete the whole data importing process.

We call the data `data region` and binlog is part of `data region`. And multiple segment are called `index region`. `data region` is sent in batches during the importer scans data source. `index region` is sent after all data are processed, and it may be sent in batches if the size exceeds the limit of the loader's rpc. When the last `index region` part is sent, the whole importing process completes.

### Bulk Load Usage

`mvn package` can be used to generate the tool jar: opemmldb-import.jar.

How to run:

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
`--help` is used to display all the configurations while `*` means this field is required.

Address of `openmldb`, database name, table name and source file must be configured properly.
Only local file in csv format with header is supported currently, and columns' names of the file must be the same with the columns' names of table. Orders of the columns' names is not necessary.

Table is not required to be existing because the importer can create a table. 

But be careful, importing data into a non-empty table will decrease the import efficient severely.

#### Configuration Adjustment

Currently, import tool can only be executed in standalone mode. Large scale data will occupy more resource, so some operations can become time-consuming.

If oom triggers, you have to increase the `-Xmx` value. 
If rpc send fails, you have to increase `rpc_write_timeout` value, and if rpc respond timeout, please increase `rpc_read_timeout` value properly.

#### Error Handling

Importing process fails when `bulk load success` not printed by the importer, or when the importer quit or when failed message is printed by the importer.

Because of the complexity of importing failure, the correct way to handle the erorr is to drop the table with failure message and rebuild table, and then import again.

You can use the import tool with configuration `-f` and then with configuration `--created_ddl` to generate the `create table statement`, and then run a new importing job.

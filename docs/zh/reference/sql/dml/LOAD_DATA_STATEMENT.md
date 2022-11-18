# LOAD DATA INFILE
`LOAD DATA INFILE`语句能高效地将文件中的数据读取到数据库中的表中。`LOAD DATA INFILE` 与 `SELECT INTO OUTFILE`互补。要将数据从 table导出到文件，请使用[SELECT INTO OUTFILE](../dql/SELECT_INTO_STATEMENT.md)。要将文件数据导入到 table 中，请使用`LOAD DATA INFILE`。
## Syntax

```sql
LoadDataInfileStmt
				::= 'LOAD' 'DATA' 'INFILE' filePath 'INTO' 'TABLE' tableName LoadDataInfileOptionsList

filePath 
				::= URI
				    
tableName
				::= string_literal

LoadDataInfileOptionsList
				::= 'OPTIONS' '(' LoadDataInfileOptionItem (',' LoadDataInfileOptionItem)* ')'

LoadDataInfileOptionItem
				::= 'DELIMITER' '=' string_literal
				|'HEADER' '=' bool_literal
				|'NULL_VALUE' '=' string_literal
				|'FORMAT' '=' string_literal
				|'QUOTE' '=' string_literal
				|'MODE' '=' string_literal
				|'DEEP_COPY' '=' bool_literal
				|'LOAD_MODE' '=' string_literal
				|'THREAD' '=' int_literal
				
URI
				::= 'file://FilePathPattern'
				|'hdfs://FilePathPattern'
				|'hive://[db.]table'
				|'FilePathPattern'

FilePathPattern
				::= string_literal
```
其中`FilePathPattern`支持通配符`*`，比如可以设成`/test/*.csv`，匹配规则和`ls FilePathPattern`一致。

支持从Hive导入数据，但需要额外的设置，详情见[Hive 支持](#hive-支持)。

下表展示了`LOAD DATA INFILE`语句的配置项。

| 配置项     | 类型    | 默认值 | 描述                                                                                                                                                                                           |
| ---------- | ------- | ------ |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delimiter  | String  | ,      | 列分隔符，默认为`,`。                                                                                                                                                                                 |
| header     | Boolean | true   | 是否包含表头, 默认为`true` 。                                                                                                                                                                          |
| null_value | String  | null   | NULL值，默认填充`"null"`。加载时，遇到null_value的字符串将被转换为`"null"`，插入表中。                                                                                                                                       |
| format     | String  | csv    | 导入文件的格式:<br />`csv`:不显示指明format时，默认为该值<br />`parquet`:集群版还支持导入parquet格式文件，单机版不支持。                                                                                                                                                              |
| quote      | String  | ""     | 输入数据的包围字符串。字符串长度<=1。默认为""，表示解析数据，不特别处理包围字符串。配置包围字符后，被包围字符包围的内容将作为一个整体解析。例如，当配置包围字符串为"#"时， `1, 1.0, #This is a string field, even there is a comma#`将为解析为三个filed.第一个是整数1，第二个是浮点1.0,第三个是一个字符串。 |
| mode       | String  | "error_if_exists" | 导入模式:<br />`error_if_exists`: 仅离线模式可用，若离线表已有数据则报错。<br />`overwrite`: 仅离线模式可用，数据将覆盖离线表数据。<br />`append`：离线在线均可用，若文件已存在，数据将追加到原文件后面。                                                           |
| deep_copy  | Boolean | true             | `deep_copy=false`仅支持离线load, 可以指定`INFILE` Path为该表的离线存储地址，从而不需要硬拷贝。                                                                                                                            |
| load_mode  | String  | cluster          | `load_mode='local'`仅支持从csv本地文件导入在线存储, 它通过本地客户端同步插入数据；<br /> `load_mode='cluster'`仅支持集群版, 通过spark插入数据，支持同步或异步模式                                                                               |
| thread     | Integer | 1                | 仅在本地文件导入时生效，即`load_mode='local'`或者单机版，表示本地插入数据的线程数。 最大值为`50`。                                                                                                                                  |


```{note}
在集群版中，`LOAD DATA INFILE`语句会根据当前执行模式（execute_mode）决定将数据导入到在线或离线存储。单机版中没有存储区别，同时也不支持`deep_copy`选项。

在线导入只能使用append模式。

离线软拷贝导入后，OpenMLDB不应修改**软连接中的数据**，因此，如果当前离线数据是软连接，就不再支持`append`方式导入。并且，当前软连接的情况下，使用`overwrite`模式的硬拷贝，也不会删除软连接的数据。
```

```{warning} INFILE Path
:class: warning

在集群版中，如果`load_mode='cluster'`，`INFILE`路径的读取是由batchjob来完成的，如果是相对路径，就需要batchjob可以访问到的相对路径。

在生产环境中，batchjob的执行通常由yarn集群调度，难以确定具体的执行者。在测试环境中，如果也是多机部署，难以确定batchjob的具体执行者。

所以，请尽量使用绝对路径。单机测试中，本地文件用`file://`开头；生产环境中，推荐使用hdfs等文件系统。
```
## SQL语句模版

```sql
LOAD DATA INFILE 'file_name' INTO TABLE 'table_name' OPTIONS (key = value, ...);
```

## Examples:

从`data.csv`文件读取数据到表`t1`在线存储中。并使用`,`作为列分隔符

```sql
set @@execute_mode='online';
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS(delimiter = ',' );
```

从`data.csv`文件读取数据到表`t1`中。并使用`,`作为列分隔符， 字符串"NA"将被替换为NULL。

```sql
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS(delimiter = ',', null_value='NA');
```

将`data_path`软拷贝到表`t1`中，作为离线数据。
```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'data_path' INTO TABLE t1 OPTIONS(deep_copy=false);
```

## Hive 支持

为了支持读Hive，我们需要Hive相关的依赖和Hive配置。

### Spark Hive依赖

我们需要在Spark中编译出Hive依赖，依赖包在`assembly/target/scala-xx/jars`. 将所有依赖加入Spark的class path中。

```
./build/mvn -Pyarn -Phive -Phive-thriftserver -DskipTests clean package
```

### Hive 配置

目前只支持使用metastore服务来连接Hive。

- spark.conf

	你可以在spark conf中配置`spark.hadoop.hive.metastore.uris`。有两种方式：
	- taskmanager.properties: 在配置项 `spark.default.conf` 中加入`spark.hadoop.hive.metastore.uris=thrift://...`并重启taskmanager。
	- CLI: 在ini conf中加入此配置项，并使用`--spark_conf`启动CLI，参考[客户端Spark配置文件](../../client_config/client_spark_config.md)。

- hive-site.xml:

	你可以配置`hive-site.xml`中的`hive.metastore.uris`，并将配置文件放入Spark home的`conf/`。

	`hive-site.xml`样例：
	```
	<configuration>
		<property>
			<name>hive.metastore.uris</name>
			<!--Make sure that <value> points to the Hive Metastore URI in your cluster -->
			<value>thrift://localhost:9083</value>
			<description>URI for client to contact metastore server</description>
		</property>
	</configuration>
	```

### Hive Debug

确认`LOAD DATA`等任务是否连接到正确的Hive集群，可以通过job log来查看。

- `INFO HiveConf:`提示读取到的是哪个Hive配置文件。如果需要配置加载的细节，可以打开spark debug日志。
- 连接Hive metastore应该有` INFO metastore: Trying to connect to metastore with URI`的日志提示。连接成功会有`INFO metastore: Connected to metastore.`日志。

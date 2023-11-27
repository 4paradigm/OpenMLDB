# LOAD DATA INFILE
`LOAD DATA INFILE`语句能高效地将文件中的数据读取到数据库中的表中。`LOAD DATA INFILE` 与 `SELECT INTO OUTFILE`互补。要将数据从 table导出到文件，请使用[SELECT INTO OUTFILE](../dql/SELECT_INTO_STATEMENT.md)。要将文件数据导入到 table 中，请使用`LOAD DATA INFILE`。

```{note}
INFILE 的 filePath，既可以是单个文件名，也可以是目录，也可以使用`*`通配符。如果目录中存在多格式的文件，只会选择 LoadDataInfileOptionsList 中指定的FORMAT格式文件。具体格式等价于`DataFrameReader.read.load(String)`，可以使用spark shell来read你想要的文件路径，确认能否读入成功。
```

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

下表展示了`LOAD DATA INFILE`语句的配置项。

| 配置项      | 类型    | 默认值            | 描述                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------- | ------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| delimiter   | String  | ,                 | 列分隔符，默认为`,`。                                                                                                                                                                                                                                                                                                                                                                                                    |
| header      | Boolean | true              | 是否包含表头, 默认为`true` 。                                                                                                                                                                                                                                                                                                                                                                                            |
| null_value  | String  | null              | NULL值，默认填充`"null"`。加载时，遇到null_value的字符串将被转换为`"null"`，插入表中。                                                                                                                                                                                                                                                                                                                                   |
| format      | String  | csv               | 导入文件的格式:<br />`csv`: 不显示指明format时，默认为该值。<br />`parquet`: 集群版还支持导入parquet格式文件，单机版不支持。                                                                                                                                                                                                                                                                                             |
| quote       | String  | "                 | 输入数据的包围字符串。字符串长度<=1。<br />load_mode=`cluster`默认为双引号`"`。配置包围字符后，被包围字符包围的内容将作为一个整体解析。例如，当配置包围字符串为"#"时， `1, 1.0, #This is a string field, even there is a comma#, normal_string`将为解析为三个filed，第一个是整数1，第二个是浮点1.0，第三个是一个字符串，第四个虽然没有quote，但也是一个字符串。<br /> **local_mode=`local`默认为`\0`，不处理包围字符。** |
| mode        | String  | "error_if_exists" | 导入模式:<br />`error_if_exists`: 仅离线模式可用，若离线表已有数据则报错。<br />`overwrite`: 仅离线模式可用，数据将覆盖离线表数据。<br />`append`：离线在线均可用，若文件已存在，数据将追加到原文件后面。                                                                                                                                                                                                                |
| deep_copy   | Boolean | true              | `deep_copy=false`仅支持离线load, 可以指定`INFILE` Path为该表的离线存储地址，从而不需要硬拷贝。                                                                                                                                                                                                                                                                                                                           |
| load_mode   | String  | cluster           | `load_mode='local'`仅支持从csv本地文件导入在线存储, 它通过本地客户端同步插入数据；<br /> `load_mode='cluster'`仅支持集群版, 通过spark插入数据，支持同步或异步模式                                                                                                                                                                                                                                                        |
| thread      | Integer | 1                 | 仅在本地文件导入时生效，即`load_mode='local'`或者单机版，表示本地插入数据的线程数。 最大值为`50`。                                                                                                                                                                                                                                                                                                                       |
| writer_type | String  | single            | 集群版在线导入中插入数据的writer类型。可选值为`single`和`batch`，默认为`single`。`single`表示数据即读即写，节省内存。`batch`则是将整个rdd分区读完，确认数据类型有效性后，再写入集群，需要更多内存。在部分情况下，`batch`模式有利于筛选未写入的数据，方便重试这部分数据。                                                                                                                                                       |

```{note}
在集群版中，`LOAD DATA INFILE`语句会根据当前执行模式（execute_mode）决定将数据导入到在线或离线存储。单机版中没有存储区别，只会导入到在线存储中，同时也不支持`deep_copy`选项。
具体的规则见下文。
```

```{warning} INFILE Path
:class: warning

在集群版中，如果`load_mode='cluster'`，`INFILE`路径的读取是由batchjob来完成的，如果是相对路径，就需要batchjob可以访问到的相对路径。

在生产环境中，batchjob的执行通常由yarn集群调度，难以确定具体的执行者。在测试环境中，如果也是多机部署，难以确定batchjob的具体执行者。

所以，请尽量使用绝对路径。单机测试中，本地文件用`file://`开头；生产环境中，推荐使用hdfs等文件系统。
```
## SQL语句模版

```sql
LOAD DATA INFILE 'file_path' INTO TABLE 'table_name' OPTIONS (key = value, ...);
```

## Hive 支持

OpenMLDB 支持从 Hive 导入数据，但需要额外的设置和功能限制，详情见 [Hive 支持](../../integration/offline_data_sources/hive.md)。

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

在离线模式下导入 Hive 数据仓库的表格：

```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'hive://db1.t1' INTO TABLE t1;
```

## 在线导入规则

在线导入只允许`mode='append'`，无法`overwrite`或`error_if_exists`。

## 离线导入规则

表的离线信息可通过`desc <table>`查看。我们将数据地址分为两类，离线地址是OpenMLDB的内部存储路径，硬拷贝将写入此地址，仅一个；软链接地址是软链接导入的地址列表。
根据模式的不同，对离线信息的修改也不同。
- overwrite模式，将会覆盖原有的所有字段，包括离线地址、软链接地址、格式、读取选项，仅保留当前overwrite进入的信息。
 - overwrite 硬拷贝，离线地址如果存在数据将被覆盖，软链接全部清空，格式更改为内部默认格式parquet，读取选项全部清空。
 - overwrite 软拷贝，离线地址直接删除（并不删除数据），软链接覆盖为输入的链接、格式、读取选项。
- append模式，append 硬拷贝将数据写入当前离线地址，append 软拷贝需要考虑当前的格式和读取选项，如果不同，将无法append。
	- append同样的路径将被忽略，但路径需要是字符串相等的，如果不同，会作为两个软链接地址。
- errorifexists，如果当前已有离线信息，将报错。这里的离线信息包括离线地址和软链接地址，比如，当前存在离线地址，无软链接，现在`LOAD DATA`软链接，也将报错。

````{tip}
如果当前离线信息存在问题，无法通过`LOAD DATA`修改，可以手动删除离线地址的数据，并用nameserver http请求清空表的离线信息。
清空离线信息步骤：
```
curl http://<ns_endpoint>/NameServer/ShowTable -d'{"db":"<db_name>","name":"<table_name>"}' # 获得其中的表tid
curl http://<ns_endpoint>/NameServer/UpdateOfflineTableInfo -d '{"db":"<db_name>","name":"<table_name>","tid":<tid>}'
```
````

由于硬拷贝的写入格式无法修改，是parquet格式，所以如果想要硬拷贝和软链接同时存在，需要保证软链接的数据格式也是parquet。

## CSV源数据格式说明

导入支持csv和parquet两种数据格式，csv的格式需要特别注意，下面举例说明。

```
c1, c2
,
"",""
ab,cd
"ef","gh"
null,null
```
这个csv源数据中，第一行两个空值（blank value）。
- cluster模式空值会被当作`null`（无论null_value是什么）。
- local模式空值会被当作空字符串，具体见[issue3015](https://github.com/4paradigm/OpenMLDB/issues/3015)。

第二行两列都是两个双引号。
- cluster模式默认quote为`"`，所以这一行是两个空字符串。
- local模式默认quote为`\0`，所以这一行两列都是两个双引号。local模式quote可以配置为`"`，但escape规则是`""`为单个`"`，和Spark不一致，具体见[issue3015](https://github.com/4paradigm/OpenMLDB/issues/3015)。

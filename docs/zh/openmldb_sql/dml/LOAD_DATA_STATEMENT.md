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

下表展示了`LOAD DATA INFILE`语句的配置项。

| 配置项     | 类型    | 默认值            | 描述                                                                                                                                                                                                                                                                                                        |
| ---------- | ------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| delimiter  | String  | ,                 | 列分隔符，默认为`,`。                                                                                                                                                                                                                                                                                       |
| header     | Boolean | true              | 是否包含表头, 默认为`true` 。                                                                                                                                                                                                                                                                               |
| null_value | String  | null              | NULL值，默认填充`"null"`。加载时，遇到null_value的字符串将被转换为`"null"`，插入表中。                                                                                                                                                                                                                      |
| format     | String  | csv               | 导入文件的格式:<br />`csv`:不显示指明format时，默认为该值<br />`parquet`:集群版还支持导入parquet格式文件，单机版不支持。                                                                                                                                                                                    |
| quote      | String  | ""                | 输入数据的包围字符串。字符串长度<=1。默认为""，表示解析数据，不特别处理包围字符串。配置包围字符后，被包围字符包围的内容将作为一个整体解析。例如，当配置包围字符串为"#"时， `1, 1.0, #This is a string field, even there is a comma#`将为解析为三个filed.第一个是整数1，第二个是浮点1.0,第三个是一个字符串。 |
| mode       | String  | "error_if_exists" | 导入模式:<br />`error_if_exists`: 仅离线模式可用，若离线表已有数据则报错。<br />`overwrite`: 仅离线模式可用，数据将覆盖离线表数据。<br />`append`：离线在线均可用，若文件已存在，数据将追加到原文件后面。                                                                                                   |
| deep_copy  | Boolean | true              | `deep_copy=false`仅支持离线load, 可以指定`INFILE` Path为该表的离线存储地址，从而不需要硬拷贝。                                                                                                                                                                                                              |
| load_mode  | String  | cluster           | `load_mode='local'`仅支持从csv本地文件导入在线存储, 它通过本地客户端同步插入数据；<br /> `load_mode='cluster'`仅支持集群版, 通过spark插入数据，支持同步或异步模式                                                                                                                                           |
| thread     | Integer | 1                 | 仅在本地文件导入时生效，即`load_mode='local'`或者单机版，表示本地插入数据的线程数。 最大值为`50`。                                                                                                                                                                                                          |


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
LOAD DATA INFILE 'file_name' INTO TABLE 'table_name' OPTIONS (key = value, ...);
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

表的离线信息可通过desc <table>查看。在没有离线信息时，进行LOAD DATA离线导入，没有特别限制。

但如果当前已有离线信息，再次LOAD DATA，能否成功和表之前的离线信息有关。规则为：
- 原信息为软链接(Deep Copy列为false)，OpenMLDB应只读该地址，不应修改**软连接中的数据**
  - 可以再次软链接，替换原软链接地址，指向别的数据地址(mode='overwrite', deep_copy=false)
  - 可以做硬拷贝(mode='overwrite', deep_copy=true)，将丢弃原软链接地址，但不会修改软链接指向的数据
- 原信息为硬拷贝(Deep Copy列为true)，数据地址(Offline path)为OpenMLDB所拥有的，可读可写
  - **不可以**替换为软链接（数据还没有回收恢复机制，直接删除是危险行为，所以暂不支持）
  - 可以再次硬拷贝(mode='overwrite'/'append', deep_copy=true)


````{tip}
如果你肯定原有的硬拷贝数据不再被需要，而现在想将离线数据地址修改为软链接，可以手动删除离线地址的数据，并用nameserver http请求清空表的离线信息。
清空离线信息步骤：
```
curl http://<ns_endpoint>/NameServer/ShowTable -d'{"db":"<db_name>","name":"<table_name>"}' # 获得其中的表tid
curl http://<ns_endpoint>/NameServer/UpdateOfflineTableInfo -d '{"db":"<db_name>","name":"<table_name>","tid":<tid>}'
```
然后，可以进行软链接导入。
````

## 导入源数据格式

导入支持csv和parquet两种数据格式。其中，csv的格式需要特别注意。

### CSV

csv源数据中，需要注意空值（blank value）。例如，
```
c1, c2
,
"",""
ab,cd
"ef","gh"
null,null
```
这个csv源数据中，第一行两个空值，cluster模式导入时会被当作`null`。第二行两列都是两个双引号，cluster模式默认quote为`"`，所以这一行是两个空字符串。

local模式下空值会被当作空字符串，具体见[issue3015](https://github.com/4paradigm/OpenMLDB/issues/3015)。

# LOAD DATA INFILE 语句

## Syntax

```sql
LoadDataInfileStmt
								::= 'LOAD' 'DATA' 'INFILE' filePath LoadDataInfileOptionsList
filePath ::= string_literal
LoadDataInfileOptionsList
									::= 'OPTIONS' '(' LoadDataInfileOptionItem (',' LoadDataInfileOptionItem)* ')'

LoadDataInfileOptionItem
									::= 'DELIMITER' '=' string_literal
											|'HEADER' '=' bool_literal
											|'NULL_VALUE' '=' string_literal
											|'FORMAT' '=' string_literal						
```

`LOAD DATA INFILE`语句以非常高的速度将文件中的行读取到 table 中。`LOAD DATA INFILE` 与 `SELECT ... INTO OUTFILE`互补。要将数据从 table 写入文件，请使用[SELECT...INTO OUTFILE](../dql/SELECT_INTO_STATEMENT.md))。要将文件读回到 table 中，请使用`LOAD DATA INFILE`。两条语句的大部分配置项相同，具体包括：

| 配置项     | 类型    | 默认值 | 描述                                                         |
| ---------- | ------- | ------ | ------------------------------------------------------------ |
| delimiter  | String  | ,      | 列分隔符，默认为`,`                                          |
| header     | Boolean | true   | 是否包含表头, 默认为`true`                                   |
| null_value | String  | null   | NULL值，默认填充`"null"`。加载时，遇到null_value的字符串将被转换为NULL，插入表中。 |
| format     | String  | csv    | 加载文件的格式，默认为`csv`。请补充一下其他的可选格式。      |
| quote      | String  | ""     | 输入数据的包围字符串。字符串长度<=1。默认为""，表示解析数据，不特别处理包围字符串。配置包围字符后，被包围字符包围的内容将作为一个整体解析。例如，当配置包围字符串为"#"时， `1, 1.0, #This is a string field, even there is a comma#`将为解析为三个filed.第一个是整数1，第二个是浮点1.0,第三个是一个字符串。 |
| mode       | String  | "error_if_exists" | 导入模式:<br />`error_if_exists`: 仅离线模式可用，若离线表已有数据则报错。<br />`overwrite`: 仅离线模式可用，数据将覆盖离线表数据。<br />`append`：离线在线均可用，若文件已存在，数据将追加到原文件后面。 |
| deep_copy  | Boolean | true   | `deep_copy=false`仅支持离线load, 可以指定`INFILE` Path为该表的离线存储地址，从而不需要硬拷贝。|

```{note}
在集群版中，`LOAD DATA INFILE`语句，根据当前执行模式（execute_mode）决定将数据导入到在线或离线存储。单机版中没有存储区别，同时也不支持`deep_copy`选项。

在线导入只能使用append模式。

离线软拷贝导入后，OpenMLDB不应修改**软连接中的数据**，因此，如果当前离线数据是软连接，就不再支持append导入。并且，当前软连接的情况下，使用overwrite模式的硬拷贝，也不会删除软连接的数据。
```

```{warning} INFILE Path
:class: warning

`INFILE`路径的读取是由batchjob来完成的，如果是相对路径，就需要batchjob可以访问到的相对路径。

在生产环境中，batchjob的执行通常是yarn集群调度，并不能确定由谁来执行。在测试环境中，如果也是多机部署，也很难确定batchjob在哪里运行。

所以，请尽量使用绝对路径。单机测试中，本地文件用`file://`开头；生产环境中，推荐使用hdfs等文件系统。
```
## SQL语句模版

```sql
LOAD DATA INFILE 'file_name' OPTIONS (key = value, ...)
```

## Examples:

从`data.csv`文件读取数据到表`t1`在线存储中。并使用`,`作为列分隔符

```sql
set @@execute_mode='online';
LOAD DATA INFILE 'data.csv' INTO TABLE t1 ( delimit = ',' );
```

从`data.csv`文件读取数据到表`t1`中。并使用`,`作为列分隔符, 字符串"NA"将被替换为NULL。

```sql
LOAD DATA INFILE 'data.csv' INTO TABLE t1 ( delimit = ',', nullptr_value='NA');
```

将`data_path`软拷贝到表`t1`中，作为离线数据。
```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'data_path' INTO TABLE t1 ( deep_copy=true );
```


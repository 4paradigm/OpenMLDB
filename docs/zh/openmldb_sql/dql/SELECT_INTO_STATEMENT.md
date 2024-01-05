# SELECT INTO
`SELECT INTO OUTFILE`语句将表的查询结果导出为一个文件。
```{note}
[LOAD DATA INFILE](../dml/LOAD_DATA_STATEMENT.md) 语句与`SELECT INTO OUTFILE`互补，它用于从指定文件创建表以及加载数据到表中。
```
## Syntax

```yacc
select_into_statement:
  query INTO OUTFILE string_file_path
  [ OPTIONS options_list ]
  [ CONFIG options_list ]

options_list:
  ( { key = value } [, ...] )
```

`SELECT INTO OUTFILE`分为三个部分。

- 第一部分是一个普通的`SELECT`语句，通过这个`SELECT`语句来查询所需要的数据；
- 第二部分是`filePath`，定义将查询的记录导出到哪个文件中，详情见[FilePath](#filepath)；
- 第三部分是`SelectIntoOptionList`为可选选项，其可能的取值有：

| 配置项     | 类型    | 默认值          | 描述                                                                                                                                                                                 |
| ---------- | ------- | --------------- |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delimiter  | String  | ,               | 列分隔符，默认为‘`,`’                                                                                                                                                                      |
| header     | Boolean | true            | 是否包含表头, 默认为`true`                                                                                                                                                                  |
| null_value | String  | null            | NULL填充值，默认填充`"null"`                                                                                                                                                               |
| format     | String  | csv             | 输出文件格式:<br />`csv`:不显示指明format时，默认为该值<br />`parquet`:集群版离线模式支持导出parquet格式文件，但集群在线和单机版不支持                                                                                                    |
| mode       | String  | error_if_exists | 输出模式:<br />`error_if_exists`: 表示若文件已经在则报错。<br />`overwrite`: 表示若文件已存在，数据将覆盖原文件内容。<br />`append`：表示若文件已存在，数据将追加到原文件后面。<br />不显示配置时，默认为`error_if_exists`。                            |
| quote      | String  | "             | 输出数据的包围字符串，字符串长度<=1。默认为双引号`"`，表示输出数据包围字符串为空。当配置包围字符串时，将使用包围字符串包围一个field。例如，我们配置包围字符串为`"#"`，原始数据为{1, 1.0, This is a string, with comma}。输出的文本为`1, 1.0, #This is a string, with comma#`。<br /> **单机模式或集群在线模式默认为`\0`，也可使用空字符串赋值，不处理包围字符。** |
| coalesce   | Int     | 0             | 仅集群版离线模式支持，默认值为0，不进行合并（可能输出多个文件），可指定最终输出几个文件。例如，coalesce=1，会将所有part合并为1个文件。 |


````{important}
请注意，目前仅有集群版支持quote字符的转义。所以，如果您使用的是单机版，请谨慎选择quote字符，保证原始字符串内并不包含quote字符。
````

## SQL语句模版

```sql
SELECT ... INTO OUTFILE 'file_path' OPTIONS (key = value, ...)
```

## FilePath

load_mode='cluster'时，FilePath支持'file://', 'hdfs://', 'hive://'三种。其中'file://'和'hdfs://'地址为目录，而非文件名，'hive://'导出到Hive表中，格式为`hive://<db>.<table>`。

单机版或load_mode='local'时，FilePath只能是file格式，且必须为文件名，不可以是目录。

## Hive 支持

OpenMLDB 支持导出数据到 Hive，但需要额外的设置和功能限制，详情见 [Hive 支持](../../integration/offline_data_sources/hive.md)。

## Examples

- 从表`t1`查询输出到`data.csv`文件中，使用`,`作为列分隔符

```SQL
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'data.csv' OPTIONS ( delimiter = ',' );
```

- 从表`t1`查询输出到`data.csv`文件中，使用`｜`作为列分隔符，NULL值的填充值为`NA`字符串：

```SQL
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'data2.csv' OPTIONS ( delimiter = '|', null_value='NA');
```

- 导出表格 t1 到 Hive 数据库

```sql
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'hive://db1.t1';
```

## Q&A

Q: select into 错误 Found duplicate column(s)?
```
Exception in thread "main" org.apache.spark.sql.AnalysisException: Found duplicate column(s) when inserting into file:/tmp/out: `c1`;
     at org.apache.spark.sql.util.SchemaUtils$.checkColumnNameDuplication(SchemaUtils.scala:90)
     at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:84)
     at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:108)
     at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:106)
     at org.apache.spark.sql.execution.command.DataWritingCommandExec.doExecute(commands.scala:131)
     at org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:175)
     at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:213)
     at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
     at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:210)
     at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:171)
     at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:122)
     at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:121)
     at org.apache.spark.sql.DataFrameWriter.$anonfun$runCommand$1(DataFrameWriter.scala:944)
```

A: 查询语句是允许列名重复的。但`SELECT INTO`除了查询还需要写入，写入中会检查重复列名。请避免重复列名，可以用`c1 as c_new`来重命名列。

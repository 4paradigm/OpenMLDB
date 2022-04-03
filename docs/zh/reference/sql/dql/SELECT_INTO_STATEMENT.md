# SELECT INTO语句

## Syntax

```sql
SelectIntoStmt
						::= SelectStmt 'INTO' 'OUTFILE' filePath SelectIntoOptionList
						
filePath ::= string_literal
SelectIntoOptionList
									::= 'OPTIONS' '(' SelectInfoOptionItem (',' SelectInfoOptionItem)* ')'

SelectInfoOptionItem
									::= 'DELIMITER' '=' string_literal
											|'HEADER' '=' bool_literal
											|'NULL_VALUE' '=' string_literal
											|'FORMAT' '=' string_literal
											|'MODE' '=' string_literal
```

`SELECT INTO OUTFILE`语句用户将表的查询结果导出为一个文件。 [`LOAD DATA INFILE`](../dml/LOAD_DATA_STATEMENT.md) 语句与`SELECT INTO OUTFILE`互补，它用于从指定文件创建表以及加载数据到表中。`SELECT INTO OUTFILE`分为三个部分。

- 第一部分是一个普通的SELECT语句，通过这个SELECT语句来查询所需要的数据；
- 第二部分是`filePath`，定义将查询的记录导出到哪个文件中；
- 第三部分是`SelectIntoOptionList`为可选选项，其可能的取值有：

| 配置项     | 类型    | 默认值          | 描述                                                         |
| ---------- | ------- | --------------- | ------------------------------------------------------------ |
| delimiter  | String  | ,               | 列分隔符，默认为`,`                                          |
| header     | Boolean | true            | 是否包含表头, 默认为`true`                                   |
| null_value | String  | null            | NULL填充值，默认填充`"null"`                                 |
| format     | String  | csv             | 输出文件格式，默认为`csv`。请补充一下其他的可选格式。        |
| mode       | String  | error_if_exists | 输出模式:<br />`error_if_exists`: 表示若文件已经在则报错。<br />`overwrite`: 表示若文件已存在，数据将覆盖原文件内容。<br />`append`：表示若文件已存在，数据将追加到原文件后面。<br />不显示配置时，默认mode为`error_if_exists`。 |
| quote      | String  | ""              | 输出数据的包围字符串，字符串长度<=1。默认为""，表示输出数据包围字符串为空。当配置包围字符串时，将使用包围字符串包围一个field。例如，我们配置包围字符串为`"#"`，原始数据为{1 1.0, This is a string, with comma}。输出的文本为`#1#, #1.0#, #This is a string, with comma#。`请注意，目前OpenMLDB还不支持quote字符的转义，所以，用户需要谨慎选择quote字符，保证原始字符串内并不包含quote字符。 |

 [`LOAD DATA INFILE`](../dml/LOAD_DATA_STATEMENT.md) 语句与`SELECT INTO OUTFILE`互补，它用户从指定文件创建表以及加载数据到表中。

## SQL语句模版

```sql
SELECT ... INTO OUTFILE 'file_path' OPTIONS (key = value, ...)
```

## Examples

- 从表`t1`查询输出到`data.csv`文件中，使用`,`作为列分隔符

```SQL
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'data.csv' OPTIONS ( delimit = ',' );
```

- 从表`t1`查询输出到`data.csv`文件中，使用`｜`作为列分隔符，NULL值的填充值为`NA`字符串：

```SQL
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'data2.csv' OPTIONS ( delimit = '|', null_value='NA');
```




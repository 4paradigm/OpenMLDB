# LOAD DATA INFILE

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

The `LOAD DATA INFILE` statement reads lines quickly from a file to a table. `LOAD DATA INFILE` is complementary to `SELECT ... INTO OUTFILE`. To write data from a table to a file, use [SELECT...INTO OUTFILE](../dql/SELECT_INTO_STATEMENT.md)). To read the file back into the table, use `LOAD DATA INFILE`. Most of the configuration items of the two statements are the same, including:


| configuration item     | type    | Defaults | describe                                                         |
| ---------- | ------- | ------ | ------------------------------------------------------------ |
| delimiter  | String  | ,      | defaul for column separator, is `,`                                          |
| header     | Boolean | true   | default to include the header, is
`true`                                   |
| null_value | String  | null   | NULL value，default population is `"null"`. When loading, strings that encounter null_value will be converted to NULL and inserted into the table. |
| format     | String  | csv    | default format of the loaded file is `csv`. Please add other optional formats.      |
| quote      | String  | ""     | A surrounding string of input data. String length <= 1. The default is "", which means parsing the data without special handling of the surrounding strings. After configuring the bracketing characters, the content surrounded by the bracketing characters will be parsed as a whole. For example, when the configuration surrounding string is "#", `1, 1.0, #This is a string field, even there is a comma#` will be parsed as three. The first is integer 1, the second is a float 1.0, and the third is a string. |
| mode | String | "error_if_exists" | Import mode:<br />`error_if_exists`: Only available in offline mode. If the offline table already has data, an error will be reported. <br />`overwrite`: Only available in offline mode, data will overwrite offline table data. <br />`append`: Available both offline and online, if the file already exists, the data will be appended to the original file. |
| deep_copy | Boolean | true | `deep_copy=false` only supports offline load, you can specify `INFILE` Path as the offline storage address of the table, so no hard copy is required.
```{note}
In the cluster version, the `LOAD DATA INFILE` statement determines whether to import data to online or offline storage according to the current execution mode (execute_mode). There is no storage difference in the stand-alone version, and the `deep_copy` option is not supported.

Online import can only use append mode.

After the offline soft copy is imported, OpenMLDB should not modify the data in the soft link. Therefore, if the current offline data is a soft link, append import is no longer supported. Moreover, in the case of the current soft connection, using the hard copy in the overwrite mode will not delete the data of the soft connection.

```

```{warning} INFILE Path
:class: warning

The reading of the `INFILE` path is done by batchjob. If it is a relative path, it needs a relative path that can be accessed by batchjob.

In a production environment, the execution of batchjobs is usually scheduled by the yarn cluster, it's not certain what executes them. In a test environment, if it's  multi-machine deployment, it becomes difficult to determine where the batchjob is running.

Please try to use absolute paths. In the stand-alone test, the local file starts with `file://`; in the production environment, it is recommended to use a file system such as hdfs.

```
## SQL Statement Template

```sql
LOAD DATA INFILE 'file_name' OPTIONS (key = value, ...)
```

## Examples:

Read data from file `data.csv` into table `t1` online storage. Use `,` as column separator

```sql
set @@execute_mode='online';
LOAD DATA INFILE 'data.csv' INTO TABLE t1 ( delimit = ',' );
```

Read data from file `data.csv` into table `t1`. Use `,` as column delimiter. The string "NA" will be replaced with NULL.

```sql
LOAD DATA INFILE 'data.csv' INTO TABLE t1 ( delimit = ',', nullptr_value='NA');
```

Soft copy `data_path` to table `t1` as offline data.
```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'data_path' INTO TABLE t1 ( deep_copy=true );
```


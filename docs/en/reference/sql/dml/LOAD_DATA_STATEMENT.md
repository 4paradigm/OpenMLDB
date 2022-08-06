# LOAD DATA INFILE

The `LOAD DATA INFILE` statement reads lines quickly from a file to a table. `LOAD DATA INFILE` is complementary to `SELECT ... INTO OUTFILE`. To write data from a table to a file, use [SELECT...INTO OUTFILE](../dql/SELECT_INTO_STATEMENT.md)). To read the file back into the table, use `LOAD DATA INFILE`. Most of the configuration items of the two statements are the same, including:

## Syntax

```sql
LoadDataInfileStmt
				::= 'LOAD' 'DATA' 'INFILE' filePath 'INTO' 'TABLE' tableName LoadDataInfileOptionsList
filePath 
				::= string_literal
				    
tableName
				::= string_literal
LoadDataInfileOptionsList
				::= 'OPTIONS' '(' LoadDataInfileOptionItem (',' LoadDataInfileOptionItem)* ')'
LoadDataInfileOptionItem
				::= 'DELIMITER' '=' string_literal
				|'HEADER' '=' bool_literal
				|'NULL_VALUE' '=' string_literal
				|'FORMAT' '=' string_literal						
```

The following table shows the options of `LOAD DATA INFILE`.

| Configuration Item | Type    | Default Value     | Note                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|--------------------|---------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delimiter          | String  | ,                 | It defines the column separator, default for `,`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| header             | Boolean | true              | It indicates that whether the imported table has a header. If the value is `true`, the origin table has a header.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| null_value         | String  | null              | It defines the string that will be used to replace the `NULL` value when loading data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| format             | String  | csv               | It defines the format of the input file.<br />`csv` is the default format. <br />`parquet` format is supported in cluster version.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| quote              | String  | ""                | It defines the string surrounding the input data. The string length should be <= 1. The default is "", which means that the string surrounding the input data is empty. When the surrounding string is configured, the content surrounded by a pair of the quote characters will be analyze as a whole. For example, if the surrounding string is `"#"` then the original data like `#1#, #1.0#, #This is a string, with comma#` will be converted to {1, 1.0, "This is a string, with comma"}.                                                                   |
| mode               | String  | "error_if_exists" | It defines the input mode.<br />`error_if_exists` is the default mode which indicates that an error will be reported if the offline table already has data. This input mode is only supported by offline execution mode.<br />`overwrite` indicates that if the file already exists, the data will overwrite the contents of the original file. This input mode is only supported by offline execution mode.<br />`append` indicates that if the table already exists, the data will be appended to the original table. Both offline and online execution mode supported this input mode. |
| deep_copy          | Boolean | true              | It defines whether `deep_copy` is used. Only offline load supports `deep_copy=false`, you can specify the `INFILE` path as the offline storage address of the table to avoid hard copy.                                                                                                                                                                                                                                                                                                                                                                                                  |

```{note}
- In the cluster version, the current execution mode (`execute_mode`) determines whether to import data to online or offline storage when the `LOAD DATA INFILE` statement is executed. For the stand-alone version, there is no difference in storage mode and the `deep_copy` option is not supported.

- As metioned in the abouve table, online execution mode only supports append input mode.

- When `deep_copy=false`, OpenMLDB doesn't support to modify the data in the soft link. Therefore, if the current offline data comes from a soft link, `append` import is no longer supported. Moreover, if current connection is soft copy, using the hard copy with `overwrite` will not delete the data of the soft connection.

```

```{warning} INFILE Path
:class: warning

The reading of the `INFILE` path is done by batchjob. If it is a relative path, it needs to be a path that can be accessed by batchjob.

In a production environment, the execution of batchjobs is usually scheduled by the yarn cluster. As a result, it's not certain which batchjob will actually perform the task. In a testing environment, if it's multi-machine deployment, it is also difficult to determine where the batchjob is running.

Please use absolute paths. In the stand-alone version, the local file path starts with `file://`. In the production environment, it is recommended to use a file system such as *HDFS*.

```
## SQL Statement Template

```sql
LOAD DATA INFILE 'file_name' INTO TABLE 'table_name' OPTIONS (key = value, ...);
```

## Examples:

The following sql example imports data from file `data.csv` into table `t1` using online storage. `data.csv` uses `,` as column separator.

```sql
set @@execute_mode='online';
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS( delimiter = ',' );
```

The following sql example imports data from file `data.csv` into table `t1`. `data.csv` uses `,` as column delimiter. The null value will be replaed by string "NA".

```sql
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS( delimiter = ',', null_value='NA');
```

Soft copy `data_path` to table `t1` as offline data.
```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'data_path' INTO TABLE t1 OPTIONS( deep_copy=true );
```


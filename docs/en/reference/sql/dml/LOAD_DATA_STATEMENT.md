# LOAD DATA INFILE

The `LOAD DATA INFILE` statement load data efficiently from a file to a table. `LOAD DATA INFILE` is complementary to `SELECT ... INTO OUTFILE`. To export data from a table to a file, use [SELECT...INTO OUTFILE](../dql/SELECT_INTO_STATEMENT.md).

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

The following table introduces the parameters of `LOAD DATA INFILE`.

| Parameter | Type    | Default Value     | Note                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------|---------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delimiter          | String  | ,                 | It defines the column separator, the default value is `,`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| header             | Boolean | true              | It indicates that whether the table to import has a header. If the value is `true`, the table has a header.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| null_value         | String  | null              | It defines the string that will be used to replace the `NULL` value when loading data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| format             | String  | csv               | It defines the format of the input file.<br />`csv` is the default format. <br />`parquet` format is supported in the cluster version.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| quote              | String  | ""                | It defines the string surrounding the input data. The string length should be <= 1. The default is "", which means that the string surrounding the input data is empty. When the surrounding string is configured, the content surrounded by a pair of the quote characters will be parsed as a whole. For example, if the surrounding string is `"#"` then the original data like `1, 1.0, #This is a string, with comma#` will be converted to three field. The first field is an integer 1, the second is a float 1.0 and the third field is a string.                                  |
| mode               | String  | "error_if_exists" | It defines the input mode.<br />`error_if_exists` is the default mode which indicates that an error will be thrown out if the offline table already has data. This input mode is only supported by the offline execution mode.<br />`overwrite` indicates that if the file already exists, the data will overwrite the contents of the original file. This input mode is only supported by the offline execution mode.<br />`append` indicates that if the table already exists, the data will be appended to the original table. Both offline and online execution modes support this input mode. |
| deep_copy          | Boolean | true              | It defines whether `deep_copy` is used. Only offline load supports `deep_copy=false`, you can specify the `INFILE` path as the offline storage address of the table to avoid hard copy.                                                                                                                                                                                                                                                                                                                                                                                                     |

```{note}
- In the cluster version, the specified execution mode (defined by `execute_mode`) determines whether to import data to online or offline storage when the `LOAD DATA INFILE` statement is executed. For the standalone version, there is no difference in storage mode and the `deep_copy` option is not supported.

- As metioned in the abouve table, online execution mode only supports append input mode.

- When `deep_copy=false`, OpenMLDB doesn't support to modify the data in the soft link. Therefore, if the current offline data comes from a soft link, `append` import is no longer supported. Moreover, if current connection is soft copy, using the hard copy with `overwrite` will not delete the data of the soft connection.

```

```{warning} INFILE Path
:class: warning

The reading of the `INFILE` path is done by a batch job. If it is a relative path, it needs to be an accessible path. However, in a production environment, the execution of batch jobs is usually scheduled by a yarn cluster. As a result, it is not deterministic that which batch job will actually perform the task. In a testing environment, if it's multi-machine deployment, it is also unable to determine where the batch job is running. 

Therefore, you are suggested to use absolute paths. In the stand-alone version, the local file path starts with `file://`. In the production environment, it is recommended to use a file system such as *HDFS*.

```
## SQL Statement Template

```sql
LOAD DATA INFILE 'file_name' INTO TABLE 'table_name' OPTIONS (key = value, ...);
```

## Example

The following sql example imports data from a file `data.csv` into a table `t1` using online storage. `data.csv` uses `,` as the column separator.

```sql
set @@execute_mode='online';
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS( delimiter = ',' );
```

The following SQL example imports data from file `data.csv` into table `t1`. `data.csv` uses `,` as the column delimiter. The null value will be replaced by a string "NA".

```sql
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS( delimiter = ',', null_value='NA');
```

The following example shows an example of soft copy.
```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'data_path' INTO TABLE t1 OPTIONS(deep_copy=false);
```



# LOAD DATA INFILE
The `LOAD DATA INFILE` statement efficiently reads data from files into tables in the database. `LOAD DATA INFILE` and `SELECT INTO OUTFILE` complement each other. To export data from a table to a file, use [SELECT INTO OUTFILE](../dql/SELECT_INTO_STATEMENT.md). To import file data into a table, use `LOAD DATA INFILE`. Note that the order of columns in the imported file schema should match the order of columns in the table schema.

```{note}
Regardless of the load_mode, the `filePath` in INFILE can be a single filename, a directory, or use the `*` wildcard.
- For load_mode=cluster, the specific format is equivalent to `DataFrameReader.read.load(String)`. You can use the spark shell to read the desired file path and confirm whether it can be successfully read. If there are multiple file formats in the directory, only files in the FORMAT specified in LoadDataInfileOptionsList will be selected.
- For load_mode=local, it uses glob to select all matching files and does not check the format of individual files. Therefore, ensure that the files meeting the conditions are all in CSV format, and it is recommended to use `*.csv` to restrict the file format.
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
The `FilePathPattern` supports wildcard character `*`, with the same match rules as `ls FilePathPattern`.

Supports loading data from Hive, but needs extra settings, see [Hive Support](#Hive-support).

The following table introduces the parameters of `LOAD DATA INFILE`.

| Parameter  | Type    | Default Value     | Note                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| ---------- | ------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| delimiter  | String  | ,                 | It defines the column separator, the default value is `,`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| header     | Boolean | true              | It indicates that whether the table to import has a header. If the value is `true`, the table has a header.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| null_value | String  | null              | It defines the string that will be used to replace the `NULL` value when loading data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| format     | String  | csv               | It defines the format of the input file.<br />`csv` is the default format. <br />`parquet` format is supported in the cluster version.                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| quote      | String  | "                | It defines the string surrounding the input data. The string length should be <= 1. <br />load_mode='cluster': default is `"`, the content surrounded by a pair of the quote characters will be parsed as a whole. For example, if the surrounding string is `"#"` then the original data like `1, 1.0, #This is a string, with comma#, normal_string` will be converted to four fields. The first field is an integer 1, the second is a float 1.0, the third field is a string "This is a string, with comma" and the 4th is "normal_string" even it's no quote. <br /> load_mode='local': default is `\0`, which means that the string surrounding the input data is empty. |
| mode       | String  | "error_if_exists" | It defines the input mode.<br />`error_if_exists` is the default mode which indicates that an error will be thrown out if the offline table already has data. This input mode is only supported by the offline execution mode.<br />`overwrite` indicates that if the file already exists, the data will overwrite the contents of the original file. This input mode is only supported by the offline execution mode.<br />`append` indicates that if the table already exists, the data will be appended to the original table. Both offline and online execution modes support this input mode. |
| deep_copy  | Boolean | true              | It defines whether `deep_copy` is used. Only offline load supports `deep_copy=false`, you can specify the `INFILE` path as the offline storage address of the table to avoid hard copy.                                                                                                                                                                                                                                                                                                                                                                                                            |
| load_mode  | String  | cluster           | `load_mode='local'` only supports loading the `csv` local files into the `online` storage; It loads the data synchronously by the client process. <br /> `load_mode='cluster'` only supports the cluster version. It loads the data via Spark synchronously or asynchronously.                                                                                                                                                                                                                                                                                                                     |
| thread     | Integer | 1                 | It only works for data loading locally, i.e., `load_mode='local'` or in the standalone version; It defines the number of threads used for data loading. The max value is `50`.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| writer_type | String  | single            | The writer type for inserting data in cluster online loading. The optional values are `single` and `batch`, with the default being `single`. `single` means data is read and written on the fly, saving memory. `batch`, on the other hand, reads the entire RDD partition, confirms the data type validity, and then writes it to the cluster, requiring more memory. In some cases, the `batch` mode is advantageous for filtering data that has not been written, facilitating the retry of this portion of data.                                                                                                                                                       |
| put_if_absent | Boolean | false             | When there are no duplicate rows in the source data and it does not duplicate existing data in the table, you can use this option to avoid inserting duplicate data, especially when retrying after a job failure. Equivalent to using `INSERT OR IGNORE`. For more details, see the following. |

```{note}
In the cluster version, the `LOAD DATA INFILE` statement will determine whether to import data into online or offline storage based on the current execution mode (`execute_mode`). In the standalone version, there is no storage distinction, and data will only be imported into online storage, and the `deep_copy` option is not supported.
The specific rules are described below.
```

```{warning} INFILE Path
:class: warning

In the cluster version, if `load_mode='cluster'`, the reading of the `INFILE` path is done by the batch job. If it is a relative path, the batch job needs to access the relative path.

In a production environment, the execution of the batch job is usually scheduled by the YARN cluster, making it difficult to determine the specific executor. In a test environment, if it is also a multi-node deployment, it is difficult to determine the specific executor of the batch job.

Therefore, it is recommended to use absolute paths as much as possible. In local testing, use `file://` for local files; in a production environment, it is recommended to use HDFS or other file systems.
```
## SQL Statement Template
```sql
LOAD DATA INFILE 'file_path' INTO TABLE 'table_name' OPTIONS (key = value, ...);
```

## Hive Support

OpenMLDB supports importing data from Hive, but it requires additional settings and has certain limitations. For details, see [Hive Support](../../integration/offline_data_sources/hive.md).

## Examples:

Read data from the `data.csv` file into the `t1` table in online storage. Use `,` as the column delimiter.

```sql
set @@execute_mode='online';
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS(delimiter = ',', mode = 'append');
```

Read data from the `data.csv` file into the `t1` table. Use `,` as the column delimiter, and replace the string "NA" with NULL.

```sql
LOAD DATA INFILE 'data.csv' INTO TABLE t1 OPTIONS(delimiter = ',', mode = 'append', null_value='NA');
```

Copy the `data_path` into the `t1` table as offline data.
```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'data_path' INTO TABLE t1 OPTIONS(deep_copy=false);
```

Import tables from the Hive data warehouse in offline mode:

```sql
set @@execute_mode='offline';
LOAD DATA INFILE 'hive://db1.t1' INTO TABLE t1;
```

## Online Import Rules

Online import only allows `mode='append'` and cannot be used with `overwrite` or `error_if_exists`.

If the `insert_memory_usage_limit` session variable is set, the server will fail if the memory usage exceeds the specified value.

## Offline Import Rules

The offline information for a table can be viewed using `desc <table>`. We classify data addresses into two types: Data path and Symbolic path. The offline address Data path is the internal storage path of OpenMLDB, and a hard copy will be written to this address (only one); the symbolic address Symbolic path is the address list imported by soft links and can be multiple.
```
 --- ------- ----------- ------ ---------
  #   Field   Type        Null   Default
 --- ------- ----------- ------ ---------
  1   c1      Varchar     YES
  2   c2      Int         YES
  3   c3      BigInt      YES
  4   c4      Float       YES
  5   c5      Double      YES
  6   c6      Timestamp   YES
  7   c7      Date        YES
 --- ------- ----------- ------ ---------
 --- -------------------- ------ ---- ------ ---------------
  #   name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1705743486   c1     -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 ---------------------------------------------------------- ------------------------------------------ --------- ---------
  Data path                                                  Symbolic paths                             Format    Options
 ---------------------------------------------------------- ------------------------------------------ --------- ---------
  file:///tmp/openmldb_offline_storage/demo_db/demo_table1   file:///work/taxi-trip/data/data.parquet   parquet
 ---------------------------------------------------------- ------------------------------------------ --------- ---------

 --------------- --------------
  compress_type   storage_mode
 --------------- --------------
  NoCompress      Memory
 --------------- --------------
```
The modification of offline information varies depending on the mode.
- In `overwrite` mode, all existing fields, including offline address, symbolic address, format, and reading options, will be overwritten, retaining only the information entered in the current overwrite operation.
  - For `overwrite` hard copy, if there is data in the existing offline address, it will be overwritten. All symbolic links will be cleared, and the format will be changed to the internal default format (Parquet). Reading options will be cleared.
  - For `overwrite` soft copy, the existing offline address will be deleted (without deleting the data), and symbolic links will be replaced with the input links, format, and reading options.
- In `append` mode, `append` hard copy writes data to the current offline address. For `append` soft copy, you need to consider the current format and reading options. If they are different, appending will not be possible.
  - Paths that are identical will be ignored in `append`, but paths need to be string-equal. If they are different, they will be treated as two different symbolic link addresses.
- In `errorifexists` mode, if there is existing offline information, an error will be reported. Existing offline information includes offline addresses and symbolic addresses. For example, if there is an existing offline address and no symbolic link, attempting to `LOAD DATA` with symbolic links will result in an error.

````{tip}
If there are issues with existing offline information that cannot be modified using `LOAD DATA`, you can manually delete the data in the offline address and use a Nameserver HTTP request to clear the table's offline information.
Steps to clear offline information:
```
curl http://<ns_endpoint>/NameServer/ShowTable -d'{"db":"<db_name>","name":"<table_name>"}' # Get the table tid from here
curl http://<ns_endpoint>/NameServer/UpdateOfflineTableInfo -d '{"db":"<db_name>","name":"<table_name>","tid":<tid>}'
```
````

Due to the fact that the writing format of hard copies cannot be modified and is in Parquet format, if you want both hard copies and symbolic links to coexist, you need to ensure that the data format of symbolic links is also Parquet.

## CSV Source Data Format Explanation

Importing supports two data formats: CSV and Parquet. Special attention should be given to the CSV format, as illustrated in the examples below.

1. The column delimiter for CSV is `,` by default, and it does not allow spaces. For example, "a, b" will be interpreted as two columns: the first one is `a`, and the second one is ` b` (with a space).
   - In local mode, leading and trailing spaces around the column delimiter are trimmed. Therefore, `a, b` will be interpreted as two columns: the first one is `a`, and the second one is `b`. However, it is not recommended to rely on this behavior, as the CSV column delimiter should not have spaces on either side according to the specification.
2. Cluster and local modes handle empty values differently:
   ```
   c1, c2
   ,
   "",""
   ab,cd
   "ef","gh"
   null,null
   ```
   In this CSV source data, the first line contains two blank values.
   - In cluster mode, empty values are treated as `null` (regardless of the `null_value` setting).
   - In local mode, empty values are treated as empty strings. Refer to [issue3015](https://github.com/4paradigm/OpenMLDB/issues/3015) for more details.

   The second line contains two columns with double quotes.
   - In cluster mode, where the default quote is `"` (double quote), this line represents two empty strings.
   - In local mode, where the default quote is `\0` (null character), this line represents two double quotes. Local mode can be configured to use `"` as the quote, but the escape rule is `""` for a single `"`, which is inconsistent with Spark. Refer to [issue3015](https://github.com/4paradigm/OpenMLDB/issues/3015) for more details.

3. Cluster CSV format supports two timestamp formats, but only one format is chosen per load operation, not a mix of both. If the CSV contains both timestamp formats, the parsing will fail. The choice of format is determined by the first row of data. For example, if the first row of data is `2020-01-01 00:00:00`, all subsequent timestamps will be parsed in the `yyyy-MM-dd HH:mm:ss` format. If the first row is an integer `1577808000000`, all subsequent timestamps will be parsed as integers.
   1. Timestamps can be in string format, such as `"2020-01-01 00:00:00"`.
   2. Dates can be in the format `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss`.
4. Local CSV format only supports integer timestamps and dates in the `yyyy-MM-dd` format. For example, `2022-2-2`.
   1. Neither timestamps nor dates can be in string format; for instance, `"2020-01-01"` will result in a parsing error.
   2. Dates cannot be in the `yyyy-MM-dd HH:mm:ss` format; for instance, `2022-2-2 00:00:00` will result in a parsing error.
5. Local mode does not support quote escaping for strings. If your strings contain quote characters, it is recommended to use the cluster mode.
6. If cluster mode encounters parsing failures during CSV reading, the failed column values are set to NULL, and the import process continues. In local mode, parsing failures result in direct errors, and the import is not continued.

## PutIfAbsent Explanation

PutIfAbsent is a special option that can prevent the insertion of duplicate data. It requires only one configuration and is easy to use, making it particularly suitable for retrying data import jobs after failures. It is equivalent to using `INSERT OR IGNORE`. If you try to import data with duplicates using PutIfAbsent, some data may be lost. If you need to keep duplicate data, you should not use this option and instead consider deduplicating the data before importing. Local mode does not currently support this option.

PutIfAbsent incurs additional overhead for deduplication, so its performance is related to the complexity of deduplication:

- If the table only has a ts index, and the data volume for the same key+ts is less than 10k (for precise deduplication, each row of data under the same key+ts is compared one by one), PutIfAbsent's performance will not be significantly worse. The import time is usually within twice the regular import time.
- If the table has a time index (ts column is null) or the data volume for the same key+ts is greater than 100k, PutIfAbsent's performance will be poor. The import time may exceed ten times the regular import time, making it impractical. In such data conditions, it is recommended to deduplicate before importing.

## Local Import Mode Explanation

The load_mode can use local mode, but it has some differences compared to cluster mode. If you have deployed TaskManager, it is recommended to use cluster mode. The differences are as follows:

- Local mode only supports online and does not support offline. It also only supports CSV format and does not support Parquet format.
- CSV reading in local mode has limited support (SplitLineWithDelimiterForStrings).
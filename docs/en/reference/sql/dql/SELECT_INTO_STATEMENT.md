# SELECT INTO
The `SELECT INTO OUTFILE` statement is used to export the query results into a file. 

```{note}
 The [`LOAD DATA INFILE`](../dml/LOAD_DATA_STATEMENT.md) statement is complementary to `SELECT INTO OUTFILE`, which allows users to create a table from a specified file and load data into the table.
```
## Syntax

```sql
SelectIntoStmt
						::= SelectStmt 'INTO' 'OUTFILE' filePath SelectIntoOptionList
						
filePath 
						::= string_literal
SelectIntoOptionList
						::= 'OPTIONS' '(' SelectInfoOptionItem (',' SelectInfoOptionItem)* ')'

SelectInfoOptionItem
						::= 'DELIMITER' '=' string_literal
						|'HEADER' '=' bool_literal
						|'NULL_VALUE' '=' string_literal
						|'FORMAT' '=' string_literal
						|'MODE' '=' string_literal
```

There are three parts in `SELECT INTO OUTFILE`.
- The first part is an ordinary `SELECT` statement, which queries the data that needs to be exported.
- The second part is `filePath`, which defines the file that the data should be exported into.
- The third part is `SelectIntoOptionList`, which is an optional part, and its possible values are shown in the following table.

| Configuration Item | Type    | Default Value   | Note                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|--------------------|---------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| delimiter          | String  | ,               | It defines the column separator of the exported file.                                                                                                                                                                                                                                                                                                                                                                                                                      |
| header             | Boolean | true            | It defines whether the exported table will contain a header. It will include header for default.                                                                                                                                                                                                                                                                                                                                                                           |
| null_value         | String  | null            | It defines the padding value for NULL, which is string `null` for default.                                                                                                                                                                                                                                                                                                                                                                                                 |
| format             | String  | csv             | It defines the format of the output file.<br />`csv` is the default format. <br />`parquet` format is supported in cluster version.                                                                                                                                                                                                                                                                                                                                        |
| mode               | String  | error_if_exists | It defines the output mode.<br />`error_if_exists` is the default mode which indicates that an error will be reported if the file already exists. <br />`overwrite` indicates that if the file already exists, the data will overwrite the contents of the original file. <br />`append` indicates that if the file already exists, the data will be appended to the original file.                                                                                        |
| quote              | String  | ""              | It defines the string surrounding the output data. The string length should be <= 1. The default is "", which means that the string surrounding the output data is empty. When the surrounding string is configured, every exported field will be surrounded by this string. For example, we configure the surrounding string as `"#"` and the original data as {1, 1.0, This is a string, with comma}. The output text will be `1, 1.0, #This is a string, with comma#. ` |

````{important}
Currently, only cluster version supports the escape of quote string. Please guarantee there are not any quote characters in the original string in standalone version.
````

## SQL Statement Template

```sql
SELECT ... INTO OUTFILE 'file_path' OPTIONS (key = value, ...);
```

## Examples

- The following SQL command exports the result of a query from table `t1` into `data.csv` file, using `,` as column delimiter.

```SQL
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'data.csv' OPTIONS ( delimiter = ',' );
```

- The following SQL command exports the result of a query from table `t1` into `data.csv` file, using `|` as column delimiter and NULL values are filled with string `NA`.

```SQL
SELECT col1, col2, col3 FROM t1 INTO OUTFILE 'data2.csv' OPTIONS ( delimiter = '|', null_value='NA');
```




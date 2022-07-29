# SELECT INTO

The `SELECT INTO OUTFILE` statement allows the user to export the query results of the table to a file. 
```{note}
 The [`LOAD DATA INFILE`](../dml/LOAD_DATA_STATEMENT.md) statement is complementary to `SELECT INTO OUTFILE`, which allows the user to create a table from a specified file and load data into the table.
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

 `SELECT INTO OUTFILE` is divided into three parts.

- The first part is an ordinary SELECT statement, through which the required data is queried;
- The second part is `filePath`, which defines which file to export the queried records to;
- The third part `SelectIntoOptionList` is an optional option, and its possible values are:

| configuration item     | type    | defaults          | describe                                                         |
| ---------- | ------- | --------------- | ------------------------------------------------------------ |
| delimiter  | String  | ,               | default column separator is, `,`                                          |
| header     | Boolean | true            | default to include headers, `true`                                   |
| null_value | String  | null            | NULL default padding value,`"null"`                                 |
| format     | String  | csv             | default output file format, `csv`. Please add other optional formats.       |
| mode       | String  | error_if_exists | Output mode:<br />`error_if_exists`: Indicates that an error will be reported if the file already exists. <br />`overwrite`: Indicates that if the file already exists, the data will overwrite the contents of the original file. <br />`append`: Indicates that if the file already exists, the data will be appended to the original file. <br />When the configuration is not displayed, the default mode is `error_if_exists`. |
| quote      | String  | ""              | The output data string length it <= 1. The default is "", which means that the string surrounding the output data is empty. When a surrounding string is configured, a field will be surrounded by the surrounding string. For example, we configure the surrounding string as `"#"` and the original data as {1 1.0, This is a string, with comma}. The output text is `#1#, #1.0#, #This is a string, with comma#. `Please note that currently OpenMLDB does not support the escape of quote characters, so users need to choose quote characters carefully to ensure that the original string does not contain quote characters.|


## SQL Statement Template

```sql
SELECT ... INTO OUTFILE 'file_path' OPTIONS (key = value, ...)
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




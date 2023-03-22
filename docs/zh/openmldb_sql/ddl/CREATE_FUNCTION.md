# CREATE FUNCTION

**Syntax**

```sql
CreateFunctionStatement ::=
    'CREATE' OptAggregate 'FUNCTION' FunctionDeclaration FunctionReturns OptionsList
OptAggregate ::=
    'AGGREGATE' | /* Nothing */

FunctionDeclaration ::=
    FunctionParametersPrefix ')'

FunctionParametersPrefix ::=
    '(' FunctionParameter
    | FunctionParametersPrefix "," FunctionParameter

FunctionParameter ::=
    Identifier Type
    
FunctionReturns ::=
    'RETURNS' Type

Type ::=
    'INT' | 'INT32'
    |'SMALLINT' | 'INT16'
    |'BIGINT' | 'INT64'
    |'FLOAT'
    |'DOUBLE'
    |'TIMESTAMP'
    |'DATE'
    |'STRING' | 'VARCHAR'

```

**Example**

创建一个函数输入参数类型是string, 返回类型是string, 动态库文件为libtest_udf.so
```sql
CREATE FUNCTION cut2(x string) RETURNS string OPTIONS (FILE = 'libtest_udf.so');
```

创建一个函数输入参数类型是俩个int, 返回类型是int, 动态库文件为libtest_udf.so
```sql
CREATE FUNCTION add_one(x int, y int) RETURNS INT OPTIONS (FILE = 'libtest_udf.so');
```

创建一个输入参数是bigint, 返回类型是bigint的聚合函数，动态库文件为libtest_udf.so
```sql
CREATE AGGREGATE FUNCTION special_sum(x BIGINT) RETURNS BIGINT OPTIONS (FILE = 'libtest_udf.so');
```

创建一个输入参数是bigint并且支持null, 返回类型是bigint并且支持返回null聚合函数，动态库文件为libtest_udf.so
```sql
CREATE AGGREGATE FUNCTION count_null(x BIGINT) RETURNS BIGINT OPTIONS (FILE = 'libtest_udf.so', ARG_NULLABLE=true, RETURN_NULLABLE=true);
```
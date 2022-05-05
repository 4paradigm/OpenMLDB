# CREATE FUNCTION
## Syntax
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
## Example
创建一个函数输入参数类型是string, 返回类型是string, 动态库文件为libtest_udf.so
```sql
CREATE FUNCTION cut2(x string) RETURNS string OPTIONS (FILE = 'libtest_udf.so');
```

创建一个函数输入参数类型是俩个int, 返回类型是int, 动态库文件为libtest_udf.so
```sql
CREATE FUNCTION add_one(x int, y int) RETURNS INT OPTIONS (FILE = 'libtest_udf.so');
```

# DROP FUNCTION
## Syntax
```sql
DROP FUNCTION FunctionName
```

## Example
删除函数cut2
```sql
DROP FUNCTION cut2;
```

# SHOW FUNCTIONS

## Syntax
```sql
SHOW FUNCTIONS
```
**Description**

`SHOW FUNCTIONS`用来显示已经注册的用户自定义函数

## Example
```sql
SHOW FUNCTIONS;
```
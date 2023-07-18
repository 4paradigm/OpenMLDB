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

```{warning}
如果集群版本<=0.8.0，TabletServer部署目录中默认没有`udf`目录，启动TabletServer后再创建目录并拷贝udf动态库，将无法生效（环境变量问题）。需保证TabletServer启动前存在udf目录，如果已启动，需要将所有TabletServer重启使环境变量生效。
```

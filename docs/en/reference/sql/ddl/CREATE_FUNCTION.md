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

Create a function whose input parameter type is string, the return type is string, and the dynamic library file is libtest_udf.so
```sql
CREATE FUNCTION cut2(x string) RETURNS string OPTIONS (FILE = 'libtest_udf.so');
```

Create a function whose input parameter type is two ints, the return type is int, and the dynamic library file is libtest_udf.so
```sql
CREATE FUNCTION add_one(x int, y int) RETURNS INT OPTIONS (FILE = 'libtest_udf.so');
```

Create an aggregate function whose input parameter type is bigint, the return type is bigint, and the dynamic library file is libtest_udf.so
```sql
CREATE AGGREGATE FUNCTION special_sum(x BIGINT) RETURNS BIGINT OPTIONS (FILE = 'libtest_udf.so');
```

Create an aggregate function whose input parameter type is bigint and nullable, the return type is bigint and nullable, and the dynamic library file is libtest_udf.so
```sql
CREATE AGGREGATE FUNCTION count_null(x BIGINT) RETURNS BIGINT OPTIONS (FILE = 'libtest_udf.so', ARG_NULLABLE=true, RETURN_NULLABLE=true);
```
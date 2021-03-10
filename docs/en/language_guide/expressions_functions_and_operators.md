# Expressions, Functions and Operators

### Syntax

```SQL
/* type syntax */
sql_type: 
	{ 'INT' | 'SMALLINT' | 'BIGINT' | 'FLOAT' | 'DOUBLE' | 'STRING' | 'TIMESTAMP' | 'DATE' | 'BOOL' }
	
/*SQL expression syntax*/
sql_expr:
	{ column_ref | expr_const | sql_call_expr | sql_cast_expr | sql_unary_expr | sql_binary_expr | 
  	sql_case_when_expr | sql_between_expr| sql_in_expr | sql_like_expr }

/*column reference expression syntax*/
column_ref:
	{ column_name | relation_name '.' column_name | relation_name."*" }

/*type cast expression syntax*/
sql_cast_expr: 
	{ CAST '(' sql_expr 'AS' sql_type ')'  |  sql_type '(' sql_expr ')' }

/*const expression syntax*/
expr_const: 
	{STRING | INTNUM | LONGNUM | DOUBLENUM | FLOATNUM | BOOLVALUE | NULLX}

/* unary expression syntax */
sql_unary_expr:
	{ '!' | 'NOT' | '-' } sql_expr

/*binary expression syntax*/
sql_binary_expr:
	sql_expr  {
	'+' | '-' | '*' | '/' | 'DIV' | '%' | 'MOD' | 
	'>' | '<' | '=' | '>=' | '<=' | '<>'  |
	'AND' | '&&' | 'OR' | '||' | 'XOR' |
	'LIKE' | 'NOT LIKE' | 
	} sql_expr
	
sql_case_when_expr:
	{
		CASE value WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...] [ELSE result] END
		| CASE WHEN [condition] THEN result [WHEN [condition] THEN result ...] [ELSE result] END
	}

sql_between_expr:
	sql_expr BETWEEN sql_expr AND sql_expr

sql_in_expr:
	sql_expr [NOT] IN '(' sql_expr [, sql_expr ...] ')'

sql_like_expr:
	sql_expr [NOT] LIKE sql_expr
	
/* call function expression */
sql_call_expr: 
	function_name '(' [sql_expr, ...] ')'

```

### Limitations

| 语句类型        | 状态                                                         |
| :-------------- | :----------------------------------------------------------- |
| 常量表达式      | 已支持                                                       |
| 列表达式        | 已支持                                                       |
| 算术运算表达式  | 已支持                                                       |
| 逻辑运算表达式  | 已支持                                                       |
| 比较函数与运算  | 已支持                                                       |
| 类型运算表达式  | 已支持                                                       |
| 位运算表达式    | 部分支持                                                     |
| CASE WHEN表达式 | 已支持                                                       |
| Between表达式   | 支持中                                                       |
| IN 表达式       | 支持中                                                       |
| LIKE 表达式     | 尚未支持                                                     |
| 函数表达式      | 支持大部分SQL标准支持的UDF和UDAF函数，具体参考函数手册（待补充） |
|                 |                                                              |

### 1. Comparison Operators

| Name                 | Description                  | Example                   |
| :------------------- | :--------------------------- | ------------------------- |
| `>`                  | Greater than                 | `a > b`                   |
| `>=`                 | Greater than or equal to     | `a >= b`                  |
| <                    | Less than                    | `a < b`                   |
| `<=`                 | Less than and equal to       | `a <= b`                  |
| `=`                  | Equal                        | `a = b`                   |
| `!=` , `<>`          | Not equal                    | `a != b` or `a <> b`      |
| `IS_NULL` , `ISNULL` | test whether a value is null | `ISNULL(a)`, `IS_NULL(a)` |

### 2. Logical Operators

| Name        | Description                  | Example         |
| :---------- | :--------------------------- | :-------------- |
| `AND`, `&&` | True if both values are true | `a AND b`       |
| `OR`, `||`  | True if either value is true | `a OR b`        |
| `NOT`, `!`  | True if the value is false   | `!a` or `NOT a` |

Effect of NULL on logical operators

| `a`     | `b`    | `a AND b` | `a OR b` | `NOT a` |
| :------ | :----- | :-------- | -------- | ------- |
| `TRUE`  | `NULL` | `NULL`    | `TRUE`   | `FALSE` |
| `FALSE` | `NULL` | `FALSE`   | `NULL`   | `TRUE`  |
| `NULL`  | `NULL` | `NULL`    | `NULL`   | `NULL`  |

### 3. Arithmetic Operators

| Name       | Description                     | Example            |
| :--------- | :------------------------------ | :----------------- |
| `%`, `MOD` | Modulo operator                 | `a % b`, `a MOD b` |
| `*`        | Multiplication operator         | `a * b`            |
| `+`        | Addition operator               | `a + b`            |
| `-`        | Minus operator                  | `a - b`            |
| `-`        | Change the sign of the argument | `-a`               |
| `/`        | Division operator               | `a / b`            |
| `DIV`      | Integer division                | `a DIV b`          |

###  4. Bit Operators

| Name | Description | Example  |
| :--- | :---------- | -------- |
| `&`  | Bitwise AND | `a & b`  |
| `>>` | Right shift | `a >> 3` |
| `<<` | Left shift  | `a << 3` |

### 5. Conversion Operators

| Name                  | Description                        | Example             |
| :-------------------- | :--------------------------------- | ------------------- |
| `CAST(value AS type)` | Explicitly cast a value as a type. | `CAST(a AS BIGINT)` |
| `type_name(value)`    | Explicitly cast a value as a type. | `BIGINT(a)`         |

#### Limitations 

| src\|dist     | bool   | smallint | int    | float  | int64  | double | timestamp | date   | string |
| :------------ | :----- | :------- | :----- | :----- | :----- | :----- | :-------- | :----- | :----- |
| **bool**      | Safe   | Safe     | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| ·**smallint** | UnSafe | Safe     | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| **int**       | UnSafe | UnSafe   | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| **float**     | UnSafe | UnSafe   | UnSafe | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| **bigint**    | UnSafe | UnSafe   | UnSafe | UnSafe | Safe   | UnSafe | UnSafe    | X      | Safe   |
| **double**    | UnSafe | UnSafe   | UnSafe | UnSafe | UnSafe | Safe   | UnSafe    | X      | Safe   |
| **timestamp** | UnSafe | UnSafe   | UnSafe | UnSafe | Safe   | UnSafe | Safe      | UnSafe | Safe   |
| **date**      | UnSafe | X        | X      | X      | X      | X      | UnSafe    | Safe   | Safe   |
| **string**    | UnSafe | UnSafe   | UnSafe | UnSafe | UnSafe | UnSafe | UnSafe    | UnSafe | Safe   |


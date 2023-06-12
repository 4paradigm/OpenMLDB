# 字面值

## Null Literals

该`NULL`值表示“没有数据“。NULL值可以转换为任意类型

`NULL`价值 的概念是SQL新手常见的混淆之处，他们经常认为这 `NULL`与空字符串是一回事 `''`。不是这种情况。

### Examples

```SQL
-- SELECT NULL value from t1
SELECT NULL FROM t1;

-- CAST COL1 AS NULL
SELECT CAST(NULL as INT) as NULL_INT from t1;
```

## Bool Literals

```sql
bool_literals ::= 'TRUE' | 'FALSE'
```

常量 `TRUE` 和 `FALSE` ，它是大小写不敏感的。`TRUE`等于1, 而`FALSE`等于0。

### Examples

```SQL
-- true flag
SELECT TRUE from t1;

-- false flag
SELECT FALSE from t1;
```

## Number Literals

```sql
number_literals ::= int_literal | float_literal
int_literal ::= digit (digit)*
bigint_literal ::= int_literal 'L'
float_literal 
	::= int_literal '.' int_literal
		| '.' int-literal
		
digit ::= [0-9]
```

数字包括整数和浮点数。整数包含`int`(`int32`), `smallint`(`int16`)和`bigint`()

- 整数由一串连续的数字构成，一般按数值范围表示为整型或者长整型。
- 当一串数字结尾跟一个`L`时，可以显示表达为一个长整型
- `double`类型的小数由一个点`.`连接两串数字。其中，左边的那串数字允许为空。
- 当小数结尾跟一个`F`时，表达为一个`float`类型的小数。

### Examples

```SQL
-- interger
SELECT 123 from t1;

-- big int
SELECT 1234567890987654321 from t1;

-- big int
SELECT 123L from t1;

-- double
SELECT 3.1415926 from t1;

-- float
SELECT 3.1415926f from t1;
```

## String Literals

```sql
string_literal := '"' (charactor)* '"'
								| "'" (charactor)* '"'
```

字符串是由单引号（`'`）或双引号（`"`）字符组成的一系列字节或字符。

### Examples

```SQL
-- string with double quote
SELECT "hello" from t1;

-- string with single quote
SELECT 'hello' from t1;

-- empty string with double quote
SELECT "" from t1;

-- empty string with single qoute
SELECT '' from t1;


```

## TimeInterval Literals

时间区间是由一个整数加时间单位组成。时间单位是大小写不敏感的。

```sql
time_interval_literals ::= int_literal time_unit
time_unit ::= 'S' | 'M' | 'H' | 'D'
```

### Example

```sql
-- 30d, 1000s
SELECT col1, sum(col1) over w1 FROM t1 window as w1(PARTITION BY col0 ORDER BY std_time ROWS_RANGE BETWEEN 30d PRECEDING AND 1000s PRECEDING);
```


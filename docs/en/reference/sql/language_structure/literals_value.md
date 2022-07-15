# Literal Value

## Null Literals

The `NULL` value means "no data". NULL values ​​can be converted to any type

The concept of a `NULL` value is a common source of confusion for newcomers to SQL, who often think that `NULL` is the same thing as an empty string `''`. This is not the case.

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

The constants `TRUE` and `FALSE` are case-insensitive. `TRUE` equals 1, and `FALSE` equals 0.

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

Numbers include integers and floating-point numbers. Integers include `int`(`int32`), `smallint`(`int16`) and `bigint`()

- Integers consist of a series of consecutive numbers and are generally represented as integers or long integers according to the range of values.
- When a string of numbers ends with an `L`, it can be displayed as a long integer
- Decimals like `double` are connected by a dot `.` with two strings of numbers. The string of numbers on the left is allowed to be empty.
- When the decimal is followed by an `F`, it is expressed as a decimal of type `float`.

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

A string is a sequence of bytes or characters consisting of single quote (`'`) or double quote (`"`) characters.

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

A time interval consists of an integer plus a time unit. Time units are case-insensitive.

```sql
time_interval_literals ::= int_literal time_unit
time_unit ::= 'S' | 'M' | 'H' | 'D'
```

### Example

```sql
-- 30d, 1000s
SELECT col1, sum(col1) over w1 FROM t1 window as w1(PARTITION BY col0 ORDER BY std_time ROWS_RANGE BETWEEN 30d PRECEDING AND 1000s PRECEDING);
```


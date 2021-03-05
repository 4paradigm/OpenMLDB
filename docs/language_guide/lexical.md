# 语言结构

## Identifiers

identifier must begin with a letter or `_` and may not exceed 128 characters. In addition, all ASCII characters are converted to uppercase.

#### syntax

```SQL
[A-Za-z_][A-Za-z0-9_]
```

#### Examples

```SQL
-- identifier start with letter
SELECT 1 as COL1 FROM t1;
-- identifier start with _
SELECT 1 as _COL1 FROM t1;
-- identifier start with _
SELECT 1 as _1 FROM t1;
```

## 字面值（Literals）

### Null Literals

该`NULL`值表示“没有数据“。NULL值可以转换为任意类型

`NULL`价值 的概念是SQL新手常见的混淆之处，他们经常认为这 `NULL`与空字符串是一回事 `''`。不是这种情况。

#### Examples:

```SQL
-- SELECT NULL value from t1
SELECT NULL FROM t1;

-- CAST COL1 AS NULL
SELECT CAST(NULL as INT) as NULL_INT from t1;
```



### Bool Literals

Boolean literals are represented by the keywords `true` and `false`.

#### Examples:

```SQL
-- true flag
SELECT TRUE from t1;

-- false flag
SELECT FALSE from t1;
```



### Number Literals

数字包括整数和浮点数。整数包含`int`(`int32`), `smallint`(`int16`)和`bigint`()

Integer literals can be of type [Int32](https://docs.microsoft.com/en-us/dotnet/api/system.int32) or [Int64](https://docs.microsoft.com/en-us/dotnet/api/system.int64). An [Int32](https://docs.microsoft.com/en-us/dotnet/api/system.int32) literal is a series of numeric characters. An [Int64](https://docs.microsoft.com/en-us/dotnet/api/system.int64) literal is series of numeric characters followed by an uppercase L.

整数由一串连续的数字构成，一般按数值范围表示为整型或者长整型。

当一串数字结尾跟一个`L`时，可以显示表达为一个长整型,

#### Examples:

```SQL
-- interger
SELECT 123 from t1;

-- big int
SELECT 1234567890987654321 from t1;

-- big int
SELECT 123L from t1;

-- float/double
SELECT 3.1415926 from t1;
```



### String Literals

字符串是由单引号（`'`）或双引号（`"`）字符组成的一系列字节或字符。

#### Examples:

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



### Timestamp Literals (计划中)

```
TIMESTAMP 'YYYY-MM-DD HH:MM:SS.DDDDDDD [timezone]]'
```

#### Examples:

```SQL
-- timestamp 
SELECT TIMESTAMP'2020-10-1 12:13:14';

-- timestamp with timezone
SELECT TIMESTAMP'2020-10-1 12:13:14 Asia/Shanghai';
```



### DateTime Literals (计划中)

```SQL
DATETIME'YYYY-MM-DD HH:MM:SS'
```

#### Examples:

```SQL
-- datetime 
SELECT DATETIME'2020-10-1 12:13:14';

```



### Date Literals (计划中)

A `date` must have the format: `YYYY`-`MM`-`DD`, where `YYYY` is a four digit year value between 0001 and 9999, `MM` is the month between 1 and 12 and `DD` is the day value that is valid for the given month `MM`.

```SQL
-- date 
SELECT DATE'2006-10-1';
```



### Decimal (计划中)

## 大小写敏感(Case sensitivity)

| Category                                             | Case Sensitive? | Notes                       |
| ---------------------------------------------------- | --------------- | --------------------------- |
| Keywords                                             | No              |                             |
| Function names                                       | No              |                             |
| Table names                                          | No              |                             |
| Column names                                         | No              |                             |
| All type names except for protocol buffer type names | No              |                             |
| String values                                        | Yes             | Includes enum value strings |
| String comparisons                                   | Yes             |                             |
| `LIKE` matching                                      | Yes             |                             |



## 关键字

关键词是在SQL中有意义的词。某些关键字，如[`SELECT`](http://www.searchdoc.cn/rdbms/mysql/dev.mysql.com/doc/refman/5.7/en/select.com.coder114.cn.html)， [`DELETE`](http://www.searchdoc.cn/rdbms/mysql/dev.mysql.com/doc/refman/5.7/en/delete.com.coder114.cn.html)或 [`BIGINT`](http://www.searchdoc.cn/rdbms/mysql/dev.mysql.com/doc/refman/5.7/en/integer-types.com.coder114.cn.html)，被用作标识符时，需要使用``符合括起。关键字都是大小写不敏感的

```SQL
SELECT `JOIN` from t1;
```

| A~D                                                          | E ~ I                                                        | J ~ P                                                        | R ~ W                                                        |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| ALL AND ANY  AS ASC   BETWEEN BY CASE CAST   CREATE   CURRENT DEFAULT  DESC DISTINCT | ELSE END  EXCLUDE EXISTS  FALSE  FOLLOWING FOR FROM FULL GROUP GROUPS HASH HAVING IF IGNORE IN INNER INTERSECT INTERVAL INTO IS | JOIN LAST LAST LEFT LIKE LIMIT  MERGE  NEW NO NOT NULL NULLS OF ON OR ORDER OUTER OVER PARTITION PRECEDING PROTO | RANGE RECURSIVE RESPECT RIGHT ROLLUP ROWS ROWS_RANGE SELECT SET SOME STRUCT TABLESAMPLE THEN TO TREAT TRUE UNBOUNDED UNION  USING WHEN WHERE WINDOW WITH WITHIN |

## 注释

SQL支持单行注释：此`#`开头

SQL 支持多行注释: `/* 里面是注释的内容 */`
# Literals and Syntax

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

## Literals

### Null Literals

The null literal is used to represent the value null for any type. A null literal is compatible with any type.

Typed nulls can be created by a cast over a null literal. For more information, see [CAST](./Query.md)

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

Integer literals can be of type [Int32](https://docs.microsoft.com/en-us/dotnet/api/system.int32) or [Int64](https://docs.microsoft.com/en-us/dotnet/api/system.int64). An [Int32](https://docs.microsoft.com/en-us/dotnet/api/system.int32) literal is a series of numeric characters. An [Int64](https://docs.microsoft.com/en-us/dotnet/api/system.int64) literal is series of numeric characters followed by an uppercase L.

#### Examples:

```SQL
-- interger
SELECT 123 from t1;

-- big int
SELECT 1234567890987654321L from t1;

-- float/double
SELECT 3.1415926 from t1;
```



### String Literals

A string is a sequence of bytes or characters, enclosed within either single quote (`'`) or double quote (`"`) characters.

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



### Timestamp Literals (Planning)

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



### DateTime Literals (Planning)

```SQL
DATETIME'YYYY-MM-DD HH:MM:SS'
```

#### Examples:

```SQL
-- datetime 
SELECT DATETIME'2020-10-1 12:13:14';

```



### Date Literals (Planning)

A `date` must have the format: `YYYY`-`MM`-`DD`, where `YYYY` is a four digit year value between 0001 and 9999, `MM` is the month between 1 and 12 and `DD` is the day value that is valid for the given month `MM`.

```SQL
-- date 
SELECT DATE'2006-10-1';
```



### Decimal (Planning)

## Case sensitivity

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



## Reserved keywords

Keywords are a group of tokens that have special meaning in the SQL language, and have the following characteristics:

- Keywords cannot be used as identifiers unless enclosed by backtick (`) characters.
- Keywords are case insensitive.

| A~D                                                          | E ~ I                                                        | J ~ P                                                        | R ~ W                                                        |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| ALL AND ANY  AS ASC   BETWEEN BY CASE CAST   CREATE   CURRENT DEFAULT  DESC DISTINCT | ELSE END  EXCLUDE EXISTS  FALSE  FOLLOWING FOR FROM FULL GROUP GROUPS HASH HAVING IF IGNORE IN INNER INTERSECT INTERVAL INTO IS | JOIN LAST LAST LEFT LIKE LIMIT  MERGE  NEW NO NOT NULL NULLS OF ON OR ORDER OUTER OVER PARTITION PRECEDING PROTO | RANGE RECURSIVE RESPECT RIGHT ROLLUP ROWS ROWS_RANGE SELECT SET SOME STRUCT TABLESAMPLE THEN TO TREAT TRUE UNBOUNDED UNION  USING WHEN WHERE WINDOW WITH WITHIN |

## Comments

Comments are sequences of characters that the parser ignores. Our SQL supports the following types of comments.
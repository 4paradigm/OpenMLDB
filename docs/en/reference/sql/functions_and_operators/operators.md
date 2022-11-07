# Operator

## Operator Precedence

```yacc
%left "OR"
%left "AND"
%left "XOR"
%left UNARY_NOT_PRECEDENCE // (NOT / !)
%nonassoc "=" "==" "<>" ">" "<" ">=" "<=" "!=" "LIKE" "ILIKE" "RLIKE" "IN" "DISTINCT" "BETWEEN" "IS" "NOT_SPECIAL"
%nonassoc "ESCAPE"
%left "|"
%left "^"
%left "&"
%left "<<" ">>"
%left "+" "-"
%left "||"
%left "*" "/" "DIV" "%" "MOD"
%left UNARY_PRECEDENCE  // For all unary operators, +, -, ~
```

## Various Operations

### 1. Comparison Operation

| operator name   | function description         |
| :-------------- | :--------------------------  |
| `>`             | Greater than                 |
| `>=`            | Greater than or equal to     |
| `<`             | Less than                    |
| `<=`            | Less than or equal to        |
| `!=`, `<>`      | Not equal to                 |
| `=`, `==`       | Equal                        |
| `BEWTEEN...AND` | Between left and right       |
| `IN`            | In the collection            |
| `LIKE`          | Comparison, case sensitive   |
| `ILIKE`         | Comparison, case insensitive |
| `RLIKE`         | Regular expression comparison |

### 2. Logic Operation

| operator name    | function description |
| :----------      | :------- |
| `AND`            | Logical and   |
| `OR`             | Logical or   |
| `XOR`            | Logical xor |
| `NOT`, `!`       | Logical not, unary operator   |

### 3. Arithmetic Operations

| operator name   | function description                                |
| :---------      | :-------------------------------------------------- |
| `%`, `MOD`      | Modulo                                              |
| `*`             | Multiplication                                      |
| `+`             | Addition                                            |
| `-`             | Subtraction                                         |
| `/`             | Float division                                      |
| `DIV`           | Integer division                                    |
| `+`             | Unary plus                                          |
| `-`             | Unary minus, support only `-number`                 |

###  4. Bit Operation

| operator name | Description |
| :-------      | :---------- |
| `&`           | Bitwise AND |
| `\|`          | Bitwise OR  |
| `^`           | Bitwise XOR |
| `~`           | Bitwise NOT, unary operator |

### 5. Type Operations and Functions

| operator name       | Description                                                |
| :------------- | :--------------------------------------------------------- |
| `CAST`         | ```CAST expr AS dist_type```，cast expression `expr` to target type |
| `bool`         | `bool(expr)`，convert expression to BOOL type                       |
| `smallint`     | `smallint(expr)`，convert expression to SMALLINT type               |
| `int`          | `int(expr)`，convert expression to INT type                           |
| `bigint`       | `bigint(expr)`，convert expression to type BIGINT                   |
| `string(expr)` | `string(expr)，convert expression to type STRING`                   |

**Conversion Compatibility Between Types**

Safe: Indicates that the conversion from the original type to the target type is safe without loss of precision and no computation exceptions. For example, converting from int to bigint is unsafe:

```sql
SELECT BIGINT(12345);
-- 12345
```

Unsafe: Indicates that the conversion from the original type to the target type is unsafe, and the precision may be lost or an exception may occur after data conversion.

```sql
SELECT INT(1.2);
-- output 1
```

X：Indicates that a conversion from the original type to the target type is not supported

| src\|dist     | bool   | smallint | int    | float  | int64  | double | timestamp | date   | string |
| :------------ | :----- | :------- | :----- | :----- | :----- | :----- | :-------- | :----- | :----- |
| **bool**      | Safe   | Safe     | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| **smallint**  | UnSafe | Safe     | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| **int**       | UnSafe | UnSafe   | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| **float**     | UnSafe | UnSafe   | UnSafe | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| **bigint**    | UnSafe | UnSafe   | UnSafe | UnSafe | Safe   | UnSafe | UnSafe    | X      | Safe   |
| **double**    | UnSafe | UnSafe   | UnSafe | UnSafe | UnSafe | Safe   | UnSafe    | X      | Safe   |
| **timestamp** | UnSafe | UnSafe   | UnSafe | UnSafe | Safe   | UnSafe | Safe      | UnSafe | Safe   |
| **date**      | UnSafe | X        | X      | X      | X      | X      | UnSafe    | Safe   | Safe   |
| **string**    | UnSafe | UnSafe   | UnSafe | UnSafe | UnSafe | UnSafe | UnSafe    | UnSafe | Safe   |

## assignment operator

| operator name | function description                  |
| :------- | :------------------------ |
| `=`      | Assignment (can be used in SET statement) |

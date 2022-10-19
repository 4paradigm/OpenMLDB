# Operator

## Operator Precedence

```sql
!
- (unary minus), ~ (unary bit inversion)
^
*, /, DIV, %, MOD
-, +
<<, >>
&
|
= (comparison), <=>, >=, >, <=, <, <>, !=, IS, LIKE, REGEXP, IN
BETWEEN, CASE, WHEN, THEN, ELSE
NOT
AND, &&
XOR
OR, ||
= (assignment), :=
```

## Various Operations

### 1. Comparison Operation

| operator name        | function description               |
| :-------------- | :--------------------- |
| `>`             | greater than                   |
| `>=`            | greater than or equal to               |
| `<=`            | less than or equal to               |
| `!=` , `<>`     | not equal to                 |
| `=`             | equal                   |
| `BEWTEEN...AND` | between...and...之间       |
| `IN`            | in the collection            |
| `LIKE`          | comparison，case sensitive   |
| `ILIKE`         | comparison, not case sensitive |

### 2. Logic Operation

| operator name    | function description |
| :---------- | :------- |
| `AND`, `&&` | logical and   |
| `OR`, `||`  | logical or   |
| `XOR`       | logical and or |
| `NOT`, `!`  | logical not   |

### 3. Arithmetic Operations

| operator name   | function description                                                 |
| :--------- | :------------------------------------------------------- |
| `%`, `MOD` | Modulo operator                                          |
| `*`        | Multiplication operator                                  |
| `+`        | Addition operator                                        |
| `-`        | Minus operator                                           |
| `-`        | Change the sign of the argument只支持数值型操作数-number |
| `/`        | Division operator                                        |
| `DIV`      | Integer division                                         |

###  4. Bit Operation

| operator name | Description |
| :------- | :---------- |
| `&`      | Bitwise AND |
| `>>`     | Right shift |
| `<<`     | Left shift  |

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
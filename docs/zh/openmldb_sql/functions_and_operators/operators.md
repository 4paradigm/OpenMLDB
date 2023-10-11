# 表达式和运算符

## 运算符优先级

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

## 比较运算

| 操作符名        | 功能描述               |
| :-------------- | :--------------------- |
| `>`             | 大于                   |
| `>=`            | 大于等于               |
| `<`             | 小于                   |
| `<=`            | 小于等于               |
| `!=` , `<>`     | 不等于                 |
| `=`, `==`       | 等于                   |
| `BEWTEEN...AND` | 介于...和...之间       |
| `IN`            | 在...集合中            |
| `LIKE`          | 模糊匹配，大小写敏感   |
| `ILIKE`         | 模糊匹配, 大小写不敏感 |
| `RLIKE`         | 正则表达式匹配 |

## 逻辑运算

| 操作符名    | 功能描述 |
| :---------- | :------- |
| `AND`       | 逻辑与   |
| `OR`        | 逻辑或   |
| `XOR`       | 逻辑与或 |
| `NOT`, `!`  | 逻辑非, unary operator   |

## 算术运算

| 操作符名   | 功能描述                                                 |
| :--------- | :------------------------------------------------------- |
| `%`, `MOD` | Modulo                                                   |
| `*`        | Multiplication                                           |
| `+`        | Addition                                                 |
| `-`        | Subtraction                                              |
| `/`        | Float division                                           |
| `DIV`      | Integer division                                         |
| `+`        | Unary plus                                               |
| `-`        | Unary minus, 只支持数值型操作数-number                   |

## 位运算

| 操作符名 | Description |
| :------- | :---------- |
| `&`      | Bitwise AND |
| `\|`     | Bitwise OR  |
| `^`      | Bitwise XOR |
| `~`      | Bitwise NOT, unary operator |

## 类型运算和函数

| 操作符名       | Description                                                |
| :------------- | :--------------------------------------------------------- |
| `CAST`         | ```CAST expr AS dist_type```，将表达式`expr`强转为目标类型 |
| `bool`         | `bool(expr)`，将表达式转换BOOL类型                         |
| `smallint`     | `smallint(expr)`，将表达式转换SMALLINT类型                 |
| `int`          | `int(expr)`，将表达式转换INT类型                           |
| `bigint`       | `bigint(expr)`，将表达式转换为BIGINT类型                   |
| `string(expr)` | `string(expr)，将表达式转换为STRING类型`                   |

**类型间转换兼容情况**

Safe: 表示从从原类型转换为目标类型的转换是安全的，不会丢失精度也不会计算异常。例如，从int转成bigint不安全的：

```sql
SELECT BIGINT(12345);
-- 12345
```

Unsafe: 表示从原类型转换为目标类型的转换不安全的，数据转换后可能丢失精度或者发生异常。

```sql
SELECT INT(1.2);
-- output 1
```

X：表示从原类型转换为目标类型的转换是不支持的

| src\|dst     | bool   | smallint | int    | float  | int64  | double | timestamp | date   | string |
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

## 赋值操作符

| 操作符名 | 功能描述                  |
| :------- | :------------------------ |
| `=`      | 赋值 (可用于 SET 语句中 ) |

## 条件表达式

### CASE 表达式
    ```sql
    SELECT case 'bb' when 'aa' then 'apple' else 'nothing' end; -- SIMPLE CASE WHEN
    SELECT case
            when 'bb'='aa' then 'apple'
            when 'bb'='bb' then 'banana'
            else 'nothing' end; -- SEARCHED CASE WHEN
    ```

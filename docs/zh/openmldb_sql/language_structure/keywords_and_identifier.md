# 关键字和标识符

SQL 语句有两个基本组成部分：

- **关键字**：在 SQL 中具有特定含义的单词，例如 `CREATE`、`INDEX`、`SELECT` 和 `BOOL` 等。大小写不敏感。
- **标识符**：为 OpenMLDB 对象——数据库、表或列等所起的名称。大小写不敏感。

## 关键字

关键字构成 SQL 的词汇表，在语句中具有特定的含义（OpenMLDB 中所有的关键字请参考[关键字汇总](#关键字汇总)）。OpenMLDB 支持的 SQL 关键字分两类：

- **保留关键字**：保留关键字具有固定含义，通常不允许用作标识符，需要经过特殊处理——使用``符号包裹起才能作为标识符。
- **非保留关键字**：在某些上下文中具有特殊含义，可以用作标识符。

## 标识符

标识符最常用作数据库、表或列等对象的名称。

- 标识符可以被反引号包裹。反引号包裹保留关键字也可以成为标识符。

    ```sql
    SELECT * FROM t;  
   -- 可以写成
    SELECT * FROM `t`;

    CREATE TABLE `JOIN` (`select` INT);
 
    ```

- 不允许标识符中包含`字符。

    ```sql
    CREATE TABLE a`b (a int);
    
    Syntax error: Unclosed identifier literal [at 1:15]
    CREATE TABLE a`b (a int);
    ```

### 标识符规则

OpenMLDB SQL 语法中标识符设定规则：以 Unicode 字母或下划线 (_) 开头。后续字符可以是字母、下划线、数字(0-9) 或美元符号 ($)。

### 限定标识符/非限定标识符

OpenMLDB 中的对象名可以是限定的或者非限定的标识符。

**限定标识符**

至少包含一个限定词来描述特定的上下文环境。

- 限定词和对象名使用用 `.` 隔开。如 `db_name.tbl_name`。
- 当包含多个限定词时，限定词与限定词使用 `.` 符号隔开。如 `db_name.tbl_name.col_name`。
- `.` 的左右两端可以出现空格，`tbl_name.col_name` 等于 `tbl_name . col_name`。

**示例：**

已知系统中有两个数据库，分别为 `db0` 和 `db1`。`db1` 中有表 `t1`。当我们使用 `USE db0` 将系统的默认数据库设置为 `db0`。当使用 `db1.t1` 这个限定标识符来表示一张表时，表 `t1` 所在的上下文是在数据库 `db1`。

```sql

USE db0;

SELECT * FROM db1.t1;
```

**非限定标识符**

非限定标识符不包含任何限定词的修饰，只包含标识符本身。在给定上下文时直接使用标识符不会引起混淆的情况下，可以使用非限定标识符来表示对象。

**示例：**

`USE db1` 将系统的默认数据库设置为 `db1`。此时，可以直接使用非限定的对象名 `t1` 来代表默认数据库 `db1` 中的表 `t1`。

```
USE db1;
SELECT * from t1;
```

## 关键字汇总

以下列出了 OpenMLDB 中所有的关键字。其中保留字用 `(R)` 来标识。

[A](#a)|[B](#b)|[C](#c)|[D](#d)|[E](#e)|[F](#f)|[G](#g)|[H](#h)|[I](#i)|[J](#j)|[K](#k)|[L](#l)|[M](#m)|[N](#n)|[O](#o)|[P](#p)|[Q](#q)|[R](#r)|[S](#s)|[T](#t)|[U](#u)|[V](#v)|[W](#w)|[X](#x)|[Z](#z)

#### A

- ABORT
- ACCESS
- ACTION
- ADD
- AGGREGATE
- ALL(R)
- ALTER
- ANALYZE
- AND(R)
- ANONYMIZATION
- ANY(R)
- ARRAY(R)
- AS(R)
- ASC(R)
- ASSERT
- ASSERT_ROWS_MODIFIED(R)
- AT(R)

#### B

- BATCH
- BEGIN
- BETWEEN(R)
- BIGDECIMAL
- BIGNUMERIC
- BREAK
- BY(R)

#### C

- CALL
- CASCADE
- CASE(R)
- CAST(R)
- CHECK
- CLAMPED
- CLUSTER
- COLLATE(R)
- COLUMN
- COLUMNS
- COMMIT
- CONFIG(R)
- CONNECTION
- CONST
- CONSTANT
- CONSTRAINT
- CONTAINS(R)
- CONTINUE
- CLONE
- CREATE(R)
- CROSS(R)
- CUBE(R)
- CURRENT(R)
- CURRENT_TIME

#### D

- DATA
- DATABASE
- DATE
- DATETIME
- DECIMAL
- DECLARE
- DEFAULT(R)
- DEFINE(R)
- DEFINER
- DELETE
- DEPLOY
- DEPLOYMENT
- DESC(R)
- DESCRIBE
- DESCRIPTOR
- DETERMINISTIC
- DISTINCT(R)
- DIV(R)
- DO
- DROP

#### E

- ELSE(R)
- ELSEIF
- END(R)
- ENFORCED
- ENUM(R)
- ERROR
- ESCAPE(R)
- EXCEPT(R)
- EXCEPTION
- EXCLUDE(R)
- EXECUTE
- EXISTS(R)
- EXPLAIN
- EXPORT
- EXTERNAL
- EXTRACT(R)

#### F

- FALSE(R)
- FETCH(R)
- FILTER
- FILTER_FIELDS
- FILL
- FIRST
- FOLLOWING(R)
- FOR(R)
- FOREIGN
- FORMAT
- FROM(R)
- FULL(R)
- FUNCTION

#### G

- GENERATED
- GLOBAL
- GRANT
- GROUP(R)
- GROUP_ROWS
- GROUPING(R)
- GROUPS(R)

#### H

- HASH(R)
- HAVING(R)
- HIDDEN

#### I

- IF(R)
- IGNORE(R)
- IMMEDIATE
- IMMUTABLE
- IMPORT
- IN(R)
- INCLUDE
- INOUT
- INDEX(R)
- INFILE
- INNER(R)
- INSERT
- INSTANCE_NOT_IN_WINDOW(R)
- INTERSECT(R)
- INTERVAL(R)
- ITERATE
- INTO(R)
- INVOKER
- IS(R)
- ISOLATION
- ILIKE(R)

#### J

- JOB
- JOIN(R)
- JSON

#### K

- KEY

#### L

- LANGUAGE
- LAST(R)
- LATERAL(R)
- LEAVE
- LEFT(R)
- LEVEL
- LIKE(R)
- LIMIT(R)
- LOAD
- LOOKUP(R)
- LOOP

#### M

- MATCH
- MATCHED
- MATERIALIZED
- MAX
- MAXSIZE
- MESSAGE
- MIN
- MOD(R)
- MODEL
- MODULE
- MERGE(R)

#### N

- NATURAL(R)
- NEW(R)
- NO(R)
- NOT(R)
- NULL(R)
- NULLS(R)
- NUMERIC

#### O

- OF(R)
- OFFSET
- ON(R)
- ONLY
- OPEN(R)
- OPTIONS
- OR(R)
- ORDER(R)
- OUT
- OUTFILE
- OUTER(R)
- OVER(R)

#### P

- PARTITION(R)
- PERCENT
- PIVOT
- POLICIES
- POLICY
- PRIMARY
- PRECEDING(R)
- PROCEDURE
- PRIVATE
- PRIVILEGES
- PROTO(R)
- PUBLIC

#### Q

- QUALIFY

#### R

- RAISE
- RANGE(R)
- READ
- RECURSIVE(R)
- REFERENCES
- RENAME
- REPEAT
- REPEATABLE
- REPLACE
- REPLACE_FIELDS
- RESPECT(R)
- RESTRICT
- RETURN
- RETURNS
- REVOKE
- RIGHT(R)
- ROLLBACK
- ROLLUP(R)
- ROW
- ROWS(R)
- ROWS_RANGE(R)
- RUN

#### S

- SAFE_CAST
- SCHEMA
- SEARCH
- SECURITY
- SELECT(R)
- SESSION
- SET(R)
- SHOW
- SIMPLE
- SOME(R)
- SOURCE
- STORING
- STOP
- SQL
- STABLE
- START
- STATUS
- STORED
- STRUCT(R)
- SYSTEM
- SYSTEM_TIME

#### T

- TABLE
- TABLESAMPLE(R)
- TARGET
- TEMP
- TEMPORARY
- THEN(R)
- TIME
- TIMESTAMP
- TO(R)
- TRANSACTION
- TRANSFORM
- TREAT(R)
- TRUE(R)
- TRUNCATE
- TYPE

#### U

- UNBOUNDED(R)
- UNION(R)
- UNNEST(R)
- UNIQUE
- UNTIL
- UPDATE
- USE
- USING(R)
- UNPIVOT

#### V

- VALUE
- VALUES
- VARIABLES
- VOLATILE
- VIEW
- VIEWS

#### W

- WEIGHT
- WHEN(R)
- WHERE(R)
- WHILE
- WINDOW(R)
- WITH(R)
- WITHIN(R)
- WRITE

#### X

- XOR(R)

#### Z

- ZONE

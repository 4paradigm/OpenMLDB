# 关键字

## 保留关键字和非保留关键字

本文介绍 OpenMLDB的关键字，并对保留关键字和非保留关键字作出区分，并汇总所有的关键字以供查询使用。

关键字是 SQL 语句中具有特殊含义的单词，例如 `SELECT`，`UPDATE`，`DELETE` 等等。关键字都是大小写不敏感的。

关键字包括保留字和非保留字：

- **非保留关键字**：能够直接作为标识符，被称为**非保留关键字**（简称**非保留字**）。
- **保留关键字**：需要经过特殊处理——使用``符合括起才能作为标识符的字，被称为**保留关键字**（简称**保留字**)。

对于保留字，必须使用反引号``包裹，才能作为标识符被使用。例如：**JOIN**是OpenMLDB定义的保留字，以下语句无法成功运行：

```sql
CREATE TABLE JOIN (a INT);
```

```bash
Syntax error: Unexpected keyword JOIN [at 1:14]
CREATE TABLE JOIN (a INT);
             ^
```

需要使用反引号包裹成``` `JOIN` ```，**JOIN**才可以作为标识符：

```sql
CREATE TABLE `JOIN` (a INT);
```

```bash
SUCCEED: Create successfully
```



对于而非保留字，则不需要特别处理也可以作为标识符。例如：**DATA**是非保留字，那么以下语句可以成功运行：

```sql
CREATE TABLE DATA (a INT);
```

```
SUCCEED: Create successfully
```

## 关键字汇总

下表列出了 OpenMLDB 中所有的关键字。其中保留字用 `(R)` 来标识。

[A](#a)|[B](#b)|[C](#c)|[D](#d)|[E](#e)|[F](#f)|[G](#G)|[H](#h)|[I](#i)|[J](#j)|[K](#k)|[L](#l)|[M](#m)|[N](#n)|[O](#o)|[P](#p)|[Q](#q)|[R](#r)|[S](#s)|[T](#t)|[U](#u)|[V](#v)|[W](#w)|[X](#x)|[Y](#y)|[Z](#z)

### A
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
### B
- BATCH
- BEGIN
- BETWEEN(R)
- BIGDECIMAL
- BIGNUMERIC
- BREAK
- BY(R)
### C
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
### D
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
### E
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
### F
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
### G
- GENERATED
- GLOBAL
- GRANT
- GROUP(R)
- GROUP_ROWS
- GROUPING(R)
- GROUPS(R)
### H
- HASH(R)
- HAVING(R)
- HIDDEN
### I
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
### J
- JOB
- JOIN(R)
- JSON
### K
- KEY
### L
- LANGUAGE
- LAST(R)
- LATERAL(R)
- LEAVE
- LEFT(R)
- LEVEL
- LIKE(R)
### I
- ILIKE(R)
### L
- LIMIT(R)
- LOAD
- LOOKUP(R)
- LOOP
### M
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
### N
- NATURAL(R)
- NEW(R)
- NO(R)
- NOT(R)
- NULL(R)
- NULLS(R)
- NUMERIC
### O
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
### P
- PARTITION(R)
- PERCENT
- PIVOT
### U
- UNPIVOT
### P
- POLICIES
- POLICY
- PRIMARY
- PRECEDING(R)
- PROCEDURE
- PRIVATE
- PRIVILEGES
- PROTO(R)
- PUBLIC
### Q
- QUALIFY
### R
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
### S
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
### T
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
### U
- UNBOUNDED(R)
- UNION(R)
- UNNEST(R)
- UNIQUE
- UNTIL
- UPDATE
- USE
- USING(R)
### V
- VALUE
- VALUES
- VARIABLES
- VOLATILE
- VIEW
- VIEWS
### W
- WEIGHT
- WHEN(R)
- WHERE(R)
- WHILE
- WINDOW(R)
- WITH(R)
- WITHIN(R)
- WRITE
### X
- XOR(R)
### Z
- ZONE
# Keywords

## Reserved and Unreserved Keywords

This page introduces OpenMLDB keywords, distinguishes reserved keywords/non-reserved keywords, and summarizes keywords for query use.

Keywords are words with special meanings in SQL statements, such as `SELECT`, `UPDATE`, `DELETE`, etc. Keywords are case-insensitive.

Keywords include reserved and non-reserved words:

- **Unreserved Keyword**: Can be used directly as an identifier, which is called **Unreserved Keyword** (abbreviated as **Unreserved Word**).
- **Reserved keywords**: Special treatment is a requirement - words that can be used as identifiers with `` match brackets are called **reserved keywords** (referred to as **reserved words**).

Reserved words must be enclosed in backticks `` to be used as identifiers. For example: **JOIN** is a reserved word defined by OpenMLDB, the following statement cannot run successfully:

```sql
CREATE TABLE JOIN (a INT);
```

```bash
Syntax error: Unexpected keyword JOIN [at 1:14]
CREATE TABLE JOIN (a INT);
             ^
```

You need to use backticks to wrap it into ``` `JOIN` ```, **JOIN** can be used as an identifier:

```sql
CREATE TABLE `JOIN` (a INT);
```

```bash
SUCCEED: Create successfully
```



For non-reserved words, they can also be used as identifiers without special treatment. For example: **DATA** is a non-reserved word, then the following statement can run successfully:

```sql
CREATE TABLE DATA (a INT);
```

```
SUCCEED: Create successfully
```

## Keyword Summary

The following table lists all the keywords in OpenMLDB. The reserved words are marked with `(R)`.

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
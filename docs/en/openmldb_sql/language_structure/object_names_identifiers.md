# SQL Object Name

This article describes object names in OpenMLDB SQL statements.

Object names are used to name all objects in OpenMLDB, including databases, tables, columns, indexes, functions, deployments, etc. In SQL statements, these objects can be referenced by identifiers.

## Identifier

Identifiers can be enclosed in backticks, i.e. `SELECT * FROM t` can also be written as `SELECT * FROM `t``. But if there is at least one special symbol in the identifier, or it is a reserved keyword, it must be enclosed in backticks to refer to the schema object it represents.

```sql
CREATE TABLE `JOIN` (`select` INT);
```

Identifiers containing the ` character are not allowed.

```sql
CREATE TABLE a`b (a int);
```

```
Syntax error: Unclosed identifier literal [at 1:15]
CREATE TABLE a`b (a int);
```



## Qualified Identifier



Object names in OpenMLDB can be qualified or unqualified identifiers.

### Qualified Object Name:

Qualified object names contain at least one qualifier to describe a specific context.

- Qualifiers and object names are separated by `.`. Such as `db_name.tbl_name`.
- When multiple qualifiers are included, the qualifiers are separated from the qualifiers by the `.` symbol. Such as `db_name.tbl_name.col_name`.
- Spaces can appear at the left and right ends of `.`, and `tbl_name.col_name` is equal to `tbl_name .col_name`.

For example, it is known that there are two databases in the system, `db0` and `db1`. There is table t1 in `db1`. When we use `USE db0` to set the default database of the system to `db0`. When using the qualified identifier `db1.t1` to represent a table, the context in which the table `t1` is located is the database `db1`.

```sql
USE db0;
SELECT * FROM db1.t1;
```

### Unqualified Object Name:

Unqualified object names do not contain any qualifier modifications, only the object identifier itself

An unqualified use of an identifier, to represent an object, is acceptable if it is not confusing to use the identifier directly in that specific context. For example, we use `USE db1` to set the system's default database to `db1`. In this case, the unqualified object name `t1` can be used directly to represent the table `t1` in the default database `db0`

```
USE db1;
SELECT * from t1;
```


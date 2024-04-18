# CALL

调用一个存储过程

## 语法

```yacc
CALL [db_name.]procedure_name (procedure_argument[, …])
```

## CALL 语句调用 Deployment

SQL [Deployment](../deployment_manage/DEPLOY_STATEMENT.md) 内部以存储过程的形式实现，它的参数是 SQL 主表的 schema, 可以通过 CALL 语句在 SQL 上直接执行验证对应的 deployment.

## Examples

```sql
-- table t1 schema is (id int, val string)
deploy dp1 select * from t1;

-- 调用存储过程 dp1, dp1 有两个参数 (int, string)
call dp1 (12, "str")
```

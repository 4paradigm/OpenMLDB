# CALL

Invoke a procedure

## Syntax

```yacc
CALL [db_name.]procedure_name (procedure_argument[, …])
```

## Use CALL statement to invoke a deployment

SQL [Deployment](../deployment_manage/DEPLOY_STATEMENT.md] is implemented internally with stored procedure, so it's natural to invoke a deployed deployment with CALL statement, required parameters are exactly the same with deployment SQL's request table schema.

## Examples

```sql
-- table t1 schema is (id int, val string)
deploy dp1 select * from t1;

-- dp1 has two parameter corresponding to table t1's schema: (int, string)
call dp1 (12, "str")
```

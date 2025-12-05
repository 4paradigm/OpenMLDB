# DROP USER

The `DROP USER` statement is used to drop a user.

## Syntax
```sql
DropUserstmt ::=
    'DROP' 'USER' [IF EXISTS] UserName

UserName ::= Identifier
```

## **Examples**
```sql
DROP USER user1;
-- SUCCEED
DROP USER IF EXISTS user2;
-- SUCCEED
```

```{note}
1. The user `root` cannot be deleted
```

## Related SQL

[CREATE USER](./CREATE_USER_STATEMENT.md)
[ALTER USER](./ALTER_USER_STATEMENT.md)
[SHOW CURRENT_USER](./SHOW_CURRENT_USER_STATEMENT.md)
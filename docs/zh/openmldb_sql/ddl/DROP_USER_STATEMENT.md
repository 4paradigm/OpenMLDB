# DROP USER

`DROP USER` 语句用来删除用户。

## 语法
```sql
DropUserstmt ::=
    'DROP' 'USER' [IF EXISTS] UserName

UserName ::= Identifier
```

## **示例**
```sql
DROP USER user1;
-- SUCCEED
DROP USER IF EXISTS user2;
-- SUCCEED
```

```{note}
1. 不能删除root用户
```

## 相关SQL

[CREATE USER](./CREATE_USER_STATEMENT.md)
[ALTER USER](./ALTER_USER_STATEMENT.md)
[SHOW CURRENT_USER](./SHOW_CURRENT_USER_STATEMENT.md)
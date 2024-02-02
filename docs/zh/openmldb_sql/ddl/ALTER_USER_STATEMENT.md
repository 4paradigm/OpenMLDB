# ALTER USER

`ALTER USER` 语句可用来修改用户密码。

## 语法
```sql
AlterUserstmt ::=
    'ALTER' 'USER' [IF EXISTS] UserName SET OptOptionsList

UserName ::= Identifier

OptOptionsList ::=
    "OPTIONS" OptionList

OptionList ::=
    OptionsListPrefix ")"

OptionsListPrefix ::=
    "(" OptionEntry
    | OptionsListPrefix "," OptionEntry

OptionEntry ::=
    Identifier "=" Identifier
```

## **示例**
```sql
ALTER USER user1;
-- SUCCEED
ALTER USER IF EXISTS user2 SET OPTIONS(password='123456');
-- SUCCEED
ALTER USER user3 SET OPTIONS (password='123456');
-- SUCCEED
```

```{note}
1. 如果不指定OPTIONS密码不会修改
2. OPTIONS中只能指定password
```

## 相关SQL

[CREATE USER](./CREATE_USER_STATEMENT.md)
[DROP USER](./DROP_USER_STATEMENT.md)
[SHOW CURRENT_USER](./SHOW_CURRENT_USER_STATEMENT.md)
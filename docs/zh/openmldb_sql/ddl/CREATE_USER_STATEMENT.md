# CREATE USER

`CREATE USER` 语句用来创建用户。

## 语法
```sql
CreateUserstmt ::=
    'CREATE' 'USER' [IF NOT EXISTS] UserName OptOptionsList

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
CREATE USER user1;
-- SUCCEED
CREATE USER IF NOT EXISTS user2;
-- SUCCEED
CREATE USER user3 OPTIONS (password='123456');
-- SUCCEED
```

```{note}
1. OPTIONS中只能指定password
2. 如果不指定password, 那么密码为空
```

## 相关SQL

[DROP USER](./DROP_USER_STATEMENT.md)
[ALTER USER](./ALTER_USER_STATEMENT.md)
[SHOW CURRENT_USER](./SHOW_CURRENT_USER_STATEMENT.md)
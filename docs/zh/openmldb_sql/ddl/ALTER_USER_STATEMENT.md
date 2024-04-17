# ALTER USER

`ALTER USER` 语句可用来修改用户密码，可以修改任意用户，包括root用户。请注意，部分服务可能和Client一样在使用用户名密码，运行中修改了其使用用户的密码，一般不影响其运行，但如果服务重启，使用旧密码登录将会失败。推荐修改密码后及时更新相关密码配置，详情见[身份认证](../../deploy/auth.md)。

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
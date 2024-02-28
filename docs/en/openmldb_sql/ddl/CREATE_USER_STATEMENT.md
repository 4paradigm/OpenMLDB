# CREATE USER

The `CREATE USER` statement is used to create a user

## Syntax
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

## **Examples**
```sql
CREATE USER user1;
-- SUCCEED
CREATE USER IF NOT EXISTS user2;
-- SUCCEED
CREATE USER user3 OPTIONS (password='123456');
-- SUCCEED
```

```{note}
1. Only the password can be specified in the OPTIONS
2. The password will be empty if not specified explicitly
```

## Related SQL

[DROP USER](./DROP_USER_STATEMENT.md)
[ALTER USER](./ALTER_USER_STATEMENT.md)
[SHOW CURRENT_USER](./SHOW_CURRENT_USER_STATEMENT.md)
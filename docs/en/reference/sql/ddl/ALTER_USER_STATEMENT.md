# ALTER USER

The `ALTER USER` statement is used to modify a user's password.

## Syntax
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

## **Examples**
```sql
ALTER USER user1;
-- SUCCEED
ALTER USER IF EXISTS user2 SET OPTIONS(password='123456');
-- SUCCEED
ALTER USER user3 SET OPTIONS (password='123456');
-- SUCCEED
```

```{note}
1. If the password is not specified in the OPTIONS, the password will not be changed
2. You can only specify the password in the OPTIONS
```

## Related SQL

[CREATE USER](./CREATE_USER_STATEMENT.md)
[DROP USER](./DROP_USER_STATEMENT.md)
[SHOW CURRENT_USER](./SHOW_CURRENT_USER_STATEMENT.md)
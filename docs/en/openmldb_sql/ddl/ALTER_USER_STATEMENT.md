# ALTER USER

The `ALTER USER` statement is used to modify a user's password. You can modify any user, including the root user. Please note that some services may use the username and password just like the client. Changing the password of the user in use will generally not affect its operation, but if the service is restarted, logging in with the old password will fail. It is recommended to update the relevant password configuration in time after modifying the password. For details, see [Authentication](../../deploy/auth.md).

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
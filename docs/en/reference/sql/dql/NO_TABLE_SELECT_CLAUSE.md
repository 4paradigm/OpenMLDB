# No Table SELECT

The no table Select statement calculates the constant expression operation list, and the expression calculation does not need to depend on the table and column.

## Syntax

```sql
NoTableSelectClause
	::= 'SELECT' SelectExprList
SelectExprList
         ::= SelectExpr ( ',' SelectExpr )*
SelectExpr    ::= ( Identifier '.' ( Identifier '.' )? )? '*'
           | ( Expression | '{' Identifier Expression '}' ) ['AS' Identifier]
     
```

## SQL Statement Template

```sql
SELECT const_expr [, const_expr ...];
```

## 2. SELECT Statement Elements

| SELECT statement elements | state                | direction                                                         |
| :------------- | ------------------- | :----------------------------------------------------------- |
| Unlabeled SELECT statement | OnlineServing not supported | The no table Select statement calculates the constant expression operation list, and the expression calculation does not need to depend on the table and column |

#### Examples

```sql
-- desc: SELECT constant literal
SELECT 1, 1L, 1.0f, 2.0, 'Hello';
-- desc: SELECT constant expression
SELECT 1+1， 1L + 1L, 1.0f - 1.0f, 2.0*2.0, 'Hello' LIKE 'He%';
-- desc: SELECT function expression
SELECT substr("hello world", 3, 6);
```
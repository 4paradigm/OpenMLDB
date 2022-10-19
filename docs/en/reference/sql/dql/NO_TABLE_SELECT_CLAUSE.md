# No-table SELECT

The no-table Select statement computes the constant expression, and the computing does not depend on tables and columns.

## Syntax

```sql
NoTableSelectClause
	::= 'SELECT' SelectExprList
SelectExprList
         ::= SelectExpr ( ',' SelectExpr )*
SelectExpr    ::= ( Identifier '.' ( Identifier '.' )? )? '*'
           | ( Expression | '{' Identifier Expression '}' ) ['AS' Identifier]
     
```

## SQL Template

```sql
SELECT const_expr [, const_expr ...];
```

## Description


| `SELECT` Statement Elements | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                    |
|:----------------------------|--------------|---------------------|---------------------|:----------------------------------------------------------------------------------------------------------------------------------------|
| No-table SELECT statement   |**``✓``** |**``✓``**  |                     | The no-table SELECT statement computes the constant expression operation list, and the computation does not depend on tables or columns |

#### Examples

SELECT constant literal
```sql
SELECT 1, 1L, 1.0f, 2.0, 'Hello';
```
SELECT constant expression
```sql
SELECT 1+1, 1L + 1L, 1.0f - 1.0f, 2.0*2.0, 'Hello' LIKE 'He%';
```
SELECT function expression
```sql
SELECT substr("hello world", 3, 6);
```
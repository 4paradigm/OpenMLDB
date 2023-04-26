# 无表 SELECT

无表Select语句计算常量表达式操作列表，表达式计算不需要依赖表和列。

## Syntax

```sql
NoTableSelectClause
	::= 'SELECT' SelectExprList
SelectExprList
         ::= SelectExpr ( ',' SelectExpr )*
SelectExpr    ::= ( Identifier '.' ( Identifier '.' )? )? '*'
           | ( Expression | '{' Identifier Expression '}' ) ['AS' Identifier]
     
```

## SQL语句模版

```sql
SELECT const_expr [, const_expr ...];
```

## 边界说明

| SELECT语句元素                                 | 离线模式  | 在线预览模式 | 在线请求模式 | 说明                                   |
| :--------------------------------------------- | --------- | ------------ | ------------ |:-------------------------------------|
| 无表SELECT语句   | **``✓``** | **``✓``**    |    | 无表Select语句计算给定的常量表达式操作列表，该计算不需要依赖表和列 |

## Examples

```sql
-- desc: SELECT 常量字面量
SELECT 1, 1L, 1.0f, 2.0, 'Hello';
-- desc: SELECT 常量表达式
SELECT 1+1, 1L + 1L, 1.0f - 1.0f, 2.0*2.0, 'Hello' LIKE 'He%';
-- desc: SELECT 函数表达式
SELECT substr("hello world", 3, 6);
```

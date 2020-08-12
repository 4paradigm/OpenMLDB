### CASE表达式

Case表达式有两种形式：

- 简单case表达式：比较`Case expr`和`When expr`, 找到第一个匹配的when表达式时，就返回结果
- 搜索Case表达式：遍历when表达式，遇到第一个为真的when表达式，就返回结果

> # CASE (Transact-SQL)
>
>  Evaluates a list of conditions and returns one of multiple possible result expressions.
>
> The CASE expression has two formats:
>
> - The simple CASE expression compares an expression to a set of simple expressions to determine the result.
> - The searched CASE expression evaluates a set of Boolean expressions to determine the result.
>
> Both formats support an optional ELSE argument.
>
> ![Topic link icon](https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/media/topic-link.gif?view=sql-server-ver15) [Transact-SQL Syntax Conventions](https://docs.microsoft.com/en-us/sql/t-sql/language-elements/transact-sql-syntax-conventions-transact-sql?view=sql-server-ver15)

```SQL
-- Simple CASE expression:   
CASE input_expression   
     WHEN when_expression THEN result_expression [ ...n ]   
     [ ELSE else_result_expression ]   
END   
-- Searched CASE expression:  
CASE  
     WHEN Boolean_expression THEN result_expression [ ...n ]   
     [ ELSE else_result_expression ]   
END  
```

### Example：

### A. Using a SELECT statement with a simple CASE expression

Within a `SELECT` statement, a simple `CASE` expression allows for only an equality check; no other comparisons are made. The following example uses the `CASE` expression to change the display of product line categories to make them more understandable.

```sql
select col1, case col0
    when 'aa' then 'apple'
    when 'bb' then 'banana'
    when 'cc' then 'cake'
    else 'nothing'
end, col2 from t1; 
```



### B. Using a SELECT statement with a searched CASE expression

Within a `SELECT` statement, the searched `CASE` expression allows for values to be replaced in the result set based on comparison values. The following example displays the list price as a text comment based on the price range for a product.

```SQL
select col1, case
    when col0='aa' then 'apple'
    when col0='bb' then 'banana'
    when col0='cc' then 'cake'
    else 'nothing'
end, col2 from t1; 
```


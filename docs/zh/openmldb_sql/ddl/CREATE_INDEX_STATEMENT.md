# CREATE INDEX

`CREATE INDEX` 语句用来创建索引。添加索引会发起异步任务来加载数据, 可以通过执行`SHOW JOBS FROM NAMESERVER`来查看任务状态

## 语法

```sql
CreateIndexstmt ::=
    'CREATE' 'INDEX' IndexName ON TableName IndexColumn OptOptionsList

IndexName ::= Identifier

TableName ::=
    Identifier ('.' Identifier)?


IndexColumn ::=
    IndexColumnPrefix ")"

IndexColumnPrefix ::=
    "(" ColumnExpression
    | IndexColumnPrefix "," ColumnExpression

ColumnExpression ::=
    Identifier
     
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
```SQL
CREATE INDEX index2 ON t5 (col2);
-- SUCCEED
```
```{note}
如果不指定Options, 创建的索引就没有指定`TS`列，因此不能用在需要上线的SQL中。
```
我们可以通过类似如下命令在创建索引时指定`TS`列:
```SQL
CREATE INDEX index3 ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d);
-- SUCCEED
```
关于`TTL`和`TTL_TYPE`的更多信息参考[这里](./CREATE_TABLE_STATEMENT.md) 

## 相关SQL

[DROP INDEX](./DROP_INDEX_STATEMENT.md)

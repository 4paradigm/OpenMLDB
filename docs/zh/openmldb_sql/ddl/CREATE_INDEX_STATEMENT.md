# CREATE INDEX

`CREATE INDEX` 语句用来创建索引。添加索引会发起异步任务来加载数据, 可以通过执行`SHOW JOBS FROM NAMESERVER`来查看任务状态。请注意，异步任务未完成之前，索引不可用，需要新索引的场景会失败。

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
1. 如果不指定Options, 创建的索引就没有指定`TS`列，因此不能用在需要上线的SQL中。
2. 指定`TS`列的类型只能是BitInt或者Timestamp
```
我们可以通过类似如下命令在创建索引时指定`TS`列:
```SQL
CREATE INDEX index3 ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d);
-- SUCCEED
```
关于`TTL`和`TTL_TYPE`的更多信息参考[这里](./CREATE_TABLE_STATEMENT.md) 

IOT表创建不同类型的索引，不指定type创建Covering索引，指定type为secondary，创建Secondary索引：
```SQL
CREATE INDEX index_s ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d, type=secondary);
```
同keys和ts列的索引被视为同一个索引，不要尝试建立不同type的同一索引。

## 相关SQL

[DROP INDEX](./DROP_INDEX_STATEMENT.md)

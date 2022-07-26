# CREATE INDEX

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

**说明**

`CREATE INDEX` 语句用来在OpenMLDB中创建索引。 如果表里有数据，添加索引会起异步任务来加载数据。通过`ns_client`中的[`showopstatus`](../../../maintain/cli.md)命令可以查看任务状态。

## **示例**
```SQL
CREATE INDEX index2 ON t5 (col2);
-- SUCCEED
```
**注**: 如果不指定Options, 创建的索引就没有指定ts列，所以不能用在需要上线的SQL中。

我们可以通过如下格式在创建索引时指定ts列:
```SQL
CREATE INDEX index3 ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d);
-- SUCCEED
```
关于`TTL`和`TTL_TYPE`的更多信息参考[这里](./CREATE_TABLE_STATEMENT.md) 

## 相关SQL

[DROP INDEX](./DROP_INDEX_STATEMENT.md)
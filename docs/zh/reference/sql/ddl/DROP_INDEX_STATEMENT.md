# DROP INDEX
`DROP INDEX`语句用来删除表中已有的索引。

## 语法

```sql
DROPIndexstmt ::=
    'DROP' 'INDEX' TableName.IndexName
```



## **示例**
```SQL
DROP INDEX t5.index2;
-- SUCCEED
```

**注**: 如果要创建已经删除的索引需要等待两个`gc_interval`的时间, 因为删除索引之后数据在gc时才会删除。`gc_interval`默认为60分钟, 可以在conf/tablet.flags配置文件中修改, 如果设置太短会导致频繁gc。

## 相关SQL

[CREATE INDEX](./CREATE_INDEX_STATEMENT.md)

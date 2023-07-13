# DROP FUNCTION

**Syntax**

```sql
DROP FUNCTION [IF EXISTS] FunctionName
```

**Example**

删除函数cut2
```sql
DROP FUNCTION cut2;
```

```{note}
删除函数实际是分布式的删除，会删除所有节点上的函数。如果某个节点删除失败，不会终止整个删除过程。我们只在整体层面，或者说是元数据层面上保证函数的唯一性，底层节点上函数可以重复创建，所以，单个节点函数删除失败不会影响后续的创建操作。但还是建议查询节点上删除失败留下的WARN日志，查看具体的删除失败的原因。
```

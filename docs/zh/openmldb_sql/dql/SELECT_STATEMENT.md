# SELECT 概况

## Syntax

### SelectStmt

```yacc
SelectStmt
         ::= WithClause ( NoTableSelectClause | SelectStmtFromTable) 

WithClause
         ::= 'WITH' non_recursive_cte [, ...]

non_recursive_cte
         ::= cte_name 'AS' '(' SelectStmt ')'

NoTableSelectClause
         ::= 'SELECT' SelectExprList      

SelectStmtFromTable
         ::= SelectStmtBasic 'FROM' TableRefs [WhereClause] [GroupByClause] [HavingClause] [WindowClause] [OrderByClause] [LimitClause]
           
JoinClause
         ::= TableRef JoinType 'JOIN' TableRef [OrderClause] 'ON' Expression 
JoinType ::= 'LAST'           
         
WhereClause
         ::= 'WHERE' Expression
         
GroupByClause
         ::= 'GROUP' 'BY' ByList

HavingClause
         ::= 'HAVING' Expression 
         
WindowClause
         ::= ( 'WINDOW' WindowDefinition ( ',' WindowDefinition )* )   
         
OrderByClause  ::= 'ORDER' 'BY' ByList   

ByList   ::= ByItem ( ',' ByItem )*

ByItem   ::= Expression Order

Order    ::= ( 'ASC' | 'DESC' )?

WindowClauseOptional
         ::= ( 'WINDOW' WindowDefinition ( ',' WindowDefinition )* )?
WindowDefinition
         ::= WindowName 'AS' WindowSpec

WindowSpec
         ::= '(' WindowSpecDetails ')'   
         
WindowSpecDetails
         ::= [ExistingWindowName] [WindowUnionClause] WindowPartitionClause WindowOrderByClause WindowFrameClause (WindowAttribute)*

WindowUnionClause
        :: = ( 'UNION' TableRefs)

WindowPartitionClause
         ::= ( 'PARTITION' 'BY' ByList ) 

WindowOrderByClause
        ::= ( 'ORDER' 'BY' ByList )

WindowFrameClause
        ::= ( WindowFrameUnits WindowFrameBounds [WindowFrameMaxSize] )

WindowFrameUnits
        ::= 'ROWS'
          | 'ROWS_RANGE'

WindowFrameBounds
        ::= 'BETWEEN' WindowFrameBound 'AND' WindowFrameBound

WindowFrameBound
        ::= ( 'UNBOUNDED' | NumLiteral | IntervalLiteral ) ['OPEN'] 'PRECEDING'
          | 'CURRENT' 'ROW'

WindowAttribute
        ::= WindowExcludeCurrentTime
          | WindowExcludeCurrentRow
          | WindowInstanceNotInWindow

WindowExcludeCurrentTime
        ::= 'EXCLUDE' 'CURRENT_TIME'

WindowExcludeCurrentRow
        ::= 'EXCLUDE' 'CURRENT_ROW'

WindowInstanceNotInWindow
        :: = 'INSTANCE_NOT_IN_WINDOW'

WindowFrameMaxSize
        :: = 'MAXSIZE' NumLiteral
```

### SelectExprList

```sql
SelectExprList
         ::= SelectExpr ( ',' SelectExpr )*
SelectExpr    ::= ( Identifier '.' ( Identifier '.' )? )? '*'
           | ( Expression | '{' Identifier Expression '}' ) ['AS' Identifier]
                      
```

### TableRefs

```sql
TableRefs
         ::= EscapedTableRef ( ',' EscapedTableRef )*
TableRef ::= TableFactor
           | JoinClause
TableFactor
         ::= TableName [TableAsName]
           | '(' ( ( SelectStmt ) ')' TableAsName | TableRefs ')' )
TableAsName
         ::= 'AS'? Identifier
```

## SELECT语句元素

| SELECT语句元素                                     | 离线模式  | 在线预览模式 | 在线请求模式 | 说明                                                                                                                                                                                                                        |
|:-----------------------------------------------| --------- | ------------ | ------------ |:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`SELECT` Clause](#selectexprlist)     | **``✓``** | **``✓``**    | **``✓``**    | 投影操作列表，一般包括列名、表达式，或者是用 `*` 表示全部列                                                                                                                                                                                          |
| [`FROM` Clause](#tablerefs)                 | **``✓``** | **``✓``**    | **``✓``**    | 表示数据来源，数据来源可以是一个表（`select * from t;`）或者是多个表 LAST JOIN (见[JOIN 子句](../dql/JOIN_CLAUSE.md)) 或者是0个表 ( `select 1+1;`)，详见[NO_TABLE SELECT](../dql/NO_TABLE_SELECT_CLAUSE.md)                                                   |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)         | **``✓``** | **``x``**    | **``✓``**    | 表示数据来源多个表JOIN。OpenMLDB目前仅支持LAST JOIN。在线请求模式下，需要遵循[Online Request下LAST JOIN的使用规范](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#在线请求模式下-last-join-的使用规范)                                                       |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)       |          | **``✓``**    |              | Where 子句用于设置过滤条件，查询结果中只会包含满足条件的数据。                                                                                                                                                                                        |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md) | **``✓``** |              |              | Group By 子句用于对查询结果集进行分组。分组表达式列表仅支持简单列。                                                                                                                                                                                    |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)     | **``✓``** |              |              | Having 子句与 Where 子句作用类似.Having 子句过滤 GroupBy 后的各种数据，Where 子句在聚合前进行过滤。                                                                                                                                                      |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)     | **``✓``** |              | **``✓``**    | 窗口子句用于定义一个或者若干个窗口。窗口可以是有名或者匿名的。用户可以在窗口上调用聚合函数来进行一些分析型计算的操作（```sql agg_func() over window_name```)。线请求模式下，需要遵循[Online Request下Window的使用规范](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#online-serving下window的使用规范) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)       | **``✓``** | **``✓``**    |              | Limit子句用于限制返回的结果条数。目前Limit仅支持接受一个参数，表示返回数据的最大行数。                                                                                                                                                                          |
| `ORDER BY` Clause                              |           |              |              | 标准SQL还支持Order By子句。OpenMLDB目前尚未支持Order子句。例如，查询语句`SELECT * from t1 ORDER BY col1;`在OpenMLDB中不被支持。                                                                                                                          |

```{warning}
在线模式或单机版的select，可能无法获取完整数据。
因为一次查询可能在多台tablet 上进行大量的扫描，为了tablet 的稳定性，单个tablet 限制了最大扫描数据量，即`scan_max_bytes_size`。

如果出现select结果截断，tablet 会出现`reach the max byte ...`的日志，但查询不会报错。

在线模式或单机版都不适合做大数据的扫描，推荐使用集群版的离线模式。如果一定要调大扫描量，需要对每台tablet配置`--scan_max_bytes_size=xxx`，并重启tablet生效。
```

## 离线同步模式 SELECT

设置`SET @@sync_job=true`后的`SELECT`语句，就是离线同步模式下的`SELECT`。在这个状态下的`SELECT`会展示结果到CLI（不建议在SDK中使用这种模式，不会得到正常的ResultSet）。

原理：SELECT执行完成后各worker通过HTTP发送结果到TaskManager，TaskManager收集各个结果分片并保存到本地文件系统中。结果收集完成后，再从本地文件系统读取，读取后删除本地缓存的结果。

```{attention}
离线同步模式 SELECT 仅用于展示，不保证结果完整。整个结果收集中可能出现文件写入失败，丢失HTTP包等问题，我们允许结果缺失。
```
### 相关配置参数

TaskManager配置`batch.job.result.max.wait.time`，在`SELECT` job完成后，我们会等待所有结果被收集并保存在TaskManager所在主机的文件系统中，超过这一时间将结束等待，返回错误。如果认为整个收集结果的过程没有问题，仅仅是等待时间不够，可以调大这一配置项，单位为ms，默认为10min。

Batch配置(spark.default.conf):
- spark.openmldb.savejobresult.rowperpost: 为了防止HTTP传送过多数据，我们对数据进行切割，默认为16000行。如果单行数据量较大，可以调小该值。
- spark.openmldb.savejobresult.posttimeouts: HTTP传送数据的超时配置，共三个超时配置项，用`,`分隔，分别为`ConnectionRequestTimeout,ConnectTimeout,SocketTimeout`，默认为`10000,10000,10000`。如果出现HTTP传输超时，可调整这一参数。

### 重置

如果使用过程中出现错误，可能导致Result Id无法正确重置。所有Result Id都被虚假占用时，会出现错误"too much running jobs to save job result, reject this spark job"。这时可以通过HTTP请求TaskManager来重置，POST内容如下：
```
curl -H "Content-Type:application/json" http://0.0.0.0:9902/openmldb.taskmanager.TaskManagerServer/SaveJobResult -X POST -d '{"result_id":-1, "json_data": "reset"}'
```

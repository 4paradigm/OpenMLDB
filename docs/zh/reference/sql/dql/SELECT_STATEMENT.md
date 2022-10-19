# SELECT 概况

## Syntax

### SelectStmt

```sql
SelectStmt
         ::= ( NoTableSelectClause | SelectStmtFromTable) 

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
         ::= [ExistingWindowName] [WindowUnionClause] WindowPartitionClause WindowFrameClause [WindowExcludeCurrentTime] [WindowInstanceNotInWindow]


WindowUnionClause
				 :: = ( 'UNION' TableRefs)

WindowPartitionClause
         ::= ( 'PARTITION' 'BY' ByList ) 

WindowFrameClause
         ::= ( WindowFrameUnits WindowFrameExtent [WindowFrameMaxSize]) 

WindowFrameUnits
         ::= 'ROWS'
           | 'ROWS_RANGE'         

WindowFrameExtent
         ::= WindowFrameStart
           | WindowFrameBetween
WindowFrameStart
         ::= ( 'UNBOUNDED' | NumLiteral | IntervalLiteral ) ['OPEN'] 'PRECEDING'
           | 'CURRENT' 'ROW'
WindowFrameBetween
         ::= 'BETWEEN' WindowFrameBound 'AND' WindowFrameBound
WindowFrameBound
         ::= WindowFrameStart
           | ( 'UNBOUNDED' | NumLiteral | IntervalLiteral ) ['OPEN'] 'FOLLOWING'  
           
WindowExcludeCurrentTime 
				::= 'EXCLUDE' 'CURRENT_TIME'      

WindowInstanceNotInWindow
				:: = 'INSTANCE_NOT_IN_WINDOW'
				
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
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)         | **``✓``** | **``✓``**    | **``✓``**    | 表示数据来源多个表JOIN。OpenMLDB目前仅支持LAST JOIN。在线请求模式下，需要遵循[Online Request下LAST JOIN的使用规范](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#online-serving下last-join的使用规范)                                                       |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)       | **``✓``** | **``✓``**    |              | Where 子句用于设置过滤条件，查询结果中只会包含满足条件的数据。                                                                                                                                                                                        |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md) | **``✓``** |              |              | Group By 子句用于对查询结果集进行分组。分组表达式列表仅支持简单列。                                                                                                                                                                                    |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)     | **``✓``** |              |              | Having 子句与 Where 子句作用类似.Having 子句过滤 GroupBy 后的各种数据，Where 子句在聚合前进行过滤。                                                                                                                                                      |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)     | **``✓``** |              | **``✓``**    | 窗口子句用于定义一个或者若干个窗口。窗口可以是有名或者匿名的。用户可以在窗口上调用聚合函数来进行一些分析型计算的操作（```sql agg_func() over window_name```)。线请求模式下，需要遵循[Online Request下Window的使用规范](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#online-serving下window的使用规范) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)       | **``✓``** | **``✓``**    | **``✓``**    | Limit子句用于限制返回的结果条数。目前Limit仅支持接受一个参数，表示返回数据的最大行数。                                                                                                                                                                          |
| `ORDER BY` Clause                              |           |              |              | 标准SQL还支持Order By子句。OpenMLDB目前尚未支持Order子句。例如，查询语句`SELECT * from t1 ORDER BY col1;`在OpenMLDB中不被支持。                                                                                                                          |

```{warning}
在线模式或单机版的select，可能无法获取完整数据。
因为一次查询可能在多台tablet 上进行大量的扫描，为了tablet 的稳定性，单个tablet 限制了最大扫描数据量，即`scan_max_bytes_size`。

如果出现select结果截断，tablet 会出现`reach the max byte ...`的日志，但查询不会报错。

在线模式或单机版都不适合做大数据的扫描，推荐使用集群版的离线模式。如果一定要调大扫描量，需要对每台tablet配置`--scan_max_bytes_size=xxx`，并重启tablet生效。
```
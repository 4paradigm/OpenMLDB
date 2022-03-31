# SELECT语句概况

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

```
SelectExprList
         ::= SelectExpr ( ',' SelectExpr )*
SelectExpr    ::= ( Identifier '.' ( Identifier '.' )? )? '*'
           | ( Expression | '{' Identifier Expression '}' ) ['AS' Identifier]
           
           
```

### TableRefs

```
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

| SELECT语句元素                                 | 状态                   | 说明                                                         |
| :--------------------------------------------- | ---------------------- | :----------------------------------------------------------- |
| `SELECT` [`SelectExprList`](#selectexprlist)   | 已支持                 | 投影操作列表，一般包括列名、表达式，或者是用 '*' 表示全部列  |
| `FROM` [`TableRefs`](#tablerefs)               | 已支持                 | 表示数据来源，数据来源可以是一个表（`select * from t;`）或者是多个表JOIN (`select * from t1 join t2;`) 或者是0个表 ( `select 1+1;`) |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)         | 仅支持LAST JOIN        | 表示数据来源多个表JOIN。OpenMLDB目前仅支持LAST JOIN。在Online Serving时，需要遵循[Online Serving下OP的使用规范](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md) |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)       | Online Serving不支持   | Where 子句用于设置过滤条件，查询结果中只会包含满足条件的数据。 |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md) | Online 不支持          | Group By 子句用于对查询结果集进行分组。分组表达式列表仅支持简单列。 |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)     | Online 不支持          | Having 子句与 Where 子句作用类似，Having 子句可以让过滤 GroupBy 后的各种数据，Where 子句用于在聚合前过滤记录。 |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)     | Online Training 不支持 | 窗口子句用于定义一个或者若干个窗口。窗口可以是有名或者匿名的。用户可以在窗口上调用聚合函数来进行一些分析型计算的操作（```sql agg_func() over window_name```)。在Online Serving时，需要遵循[Online Serving下OP的使用规范](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)       | Online Serving不支持   | Limit 子句用于限制结果条数。OpenMLDB 目前仅支持Limit 接受一个参数，表示返回数据的最大行数； |
| `ORDER BY` Clause                              | 不支持                 | 标准SQL还支持OrderBy子句。OpenMLDB目前尚未支持Order子句。例如，查询语句`SELECT * from t1 ORDER BY col1;`在OpenMLDB中不被支持。 |

```{warning}
在线模式或单机版的select，可能无法获取完整数据。
因为一次查询可能在多台tablet server上进行大量的扫描，为了tablet server的稳定性，单台tablet server限制了最大扫描数据量，即`scan_max_bytes_size`。

如果出现select结果截断，tablet server会出现`reach the max byte ...`的日志，但查询不会报错。
```
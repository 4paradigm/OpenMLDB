# SELECT Overview

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

## SELECT Statement Elements

| SELECT statement elements                                | state                   | illustrate                                                         |
| :--------------------------------------------- | ---------------------- | :----------------------------------------------------------- |
| `SELECT` [`SelectExprList`](#selectexprlist)   | supported                 | 
A list of projection operations, generally including column names, expressions, or '*' for all columns  |
| `FROM` [`TableRefs`](#tablerefs)               | supported                 | 
Indicates the data source, the data source can be one table (`select * from t;`) or multiple tables JOIN (`select * from t1 join t2;`) or 0 tables ( `select 1+1;`) |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)         | Only LAST JOIN is supported        | Indicates that the data source multiple tables JOIN. OpenMLDB currently only supports LAST JOIN. During Online Serving, you need to follow [OP's usage specification under Online Serving](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md) |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)       | Online Serving not supported   | 
The Where clause is used to set filter conditions, and only the data that meets the conditions will be included in the query result. |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md) | Online not supported          | 
The Group By clause is used to group the query result set. Grouping expression lists only support simple columns. |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)     | Online not supported          | 
The Having clause is similar to the Where clause. The Having clause allows you to filter various data after GroupBy, and the Where clause is used to filter records before aggregation. |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)     | Online Training not supported | 
The window clause is used to define one or several windows. Windows can be named or anonymous. Users can call aggregate functions on the window to perform some analytical calculations (```sql agg_func() over window_name```). During Online Serving, you need to follow [OP's usage specification under Online Serving](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)       | Online Serving does not support   | The Limit clause is used to limit the number of results. OpenMLDB currently only supports Limit accepting one parameter, indicating the maximum number of rows of returned data; |
| `ORDER BY` Clause                              | not supported                 | Standard SQL also supports the OrderBy clause. OpenMLDB does not currently support the Order clause. For example, the query `SELECT * from t1 ORDER BY col1;` is not supported in OpenMLDB. |

```{warning}
The online mode or the stand-alone version of the selection may not obtain complete data.
Because a query may perform a large number of scans on multiple tablet servers, for the stability of tablet servers, a single tablet server limits the maximum amount of scanned data, namely `scan_max_bytes_size`.

If the select result is truncated, the tablet server will display a log of `reach the max byte ...`, but the query will not report an error.
```
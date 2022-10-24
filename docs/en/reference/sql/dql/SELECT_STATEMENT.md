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

## SELECT Statement


| `SELECT` Statement and Related Clauses                    | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                                                                                                                                                                                                                                                                                                    |
|:-----------------------------------------------|--------------|---------------------|---------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`SELECT Clause`](#selectexprlist)             | **``✓``**    | **``✓``**           | **``✓``**           | A list of projection operations, generally including column names, expressions, or ‘*’ for all columns.                                                                                                                                                                                                                                                                                                                 |
| [`FROM Clause`](#tablerefs)                    | **``✓``**    | **``✓``**           | **``✓``**           | The FROM clause indicates the data source.<br />The data source can be one table (`select * from t;`) or multiple tables that LAST JOIN together (see [JOIN CLAUSE](../dql/JOIN_CLAUSE.md)) or no table ( `select 1+1;`), see [NO_TABLE SELECT](../dql/NO_TABLE_SELECT_CLAUSE.md)                                                                                                                                       |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)         | **``✓``**    | **``✓``**           | **``✓``**           | The JOIN clause indicates that the data source comes from multiple joined tables. OpenMLDB currently only supports LAST JOIN. For Online Request Mode, please follow [the specification of LAST JOIN under Online Request Mode](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#the-usage-specification-of-last-join-under-online-serving)                                                                          |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)       | **``✓``**    | **``✓``**           |                     | The WHERE clause is used to set filter conditions, and only the data that meets the conditions will be included in the query result.                                                                                                                                                                                                                                                                                    |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md) | **``✓``**    |                     |                     | The GROUP BY clause is used to group the query results.The grouping conditions only support simple columns.                                                                                                                                                                                                                                                                                                             |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)     | **``✓``**    |                     |                     | The HAVING clause is similar to the WHERE clause. The HAVING clause filters data after GROUP BY, and the WHERE clause is used to filter records before aggregation.                                                                                                                                                                                                                                                     |                                                                                                                                                                                                                                                                                                                                                                              |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)     | **``✓``**    |                     | **``✓``**           | The WINDOW clause is used to define one or several windows. Windows can be named or anonymous. Users can call aggregate functions on the window to perform analysis (```sql agg_func() over window_name```). For Online Request Mode, please follow the [specification of WINDOW Clause under Online Request Mode](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#window-usage-specification-under-online-serving) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)       | **``✓``**    | **``✓``**           |                     | The LIMIT clause is used to limit the number of results. OpenMLDB currently only supports one parameter to limit the maximum number of rows of returned data.                                                                                                                                                                                                                                                           |
| `ORDER BY` Clause                              |              |                     |                     | Standard SQL also supports the ORDER BY keyword, however OpenMLDB does not support this keyword currently. For example, the query `SELECT * from t1 ORDER BY col1;` is not supported in OpenMLDB.                                                                                                                                                                                                                       |

```{warning}
The `SELECT` running in online mode or the stand-alone version may not obtain complete data.
Because a query may perform a large number of scans on multiple tablets, for stability, the largest number of bytes to scan is limited, namely `scan_max_bytes_size`.

If the select results are truncated, the message of `reach the max byte ...` will be recorded in the tablet's log, but there will be no error.
```

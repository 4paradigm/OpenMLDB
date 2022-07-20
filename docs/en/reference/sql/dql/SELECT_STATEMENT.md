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

| `SELECT` Statement Elements                                | Offline Mode | Online Preview Mode | Online Request Mode | Illustrate                                                                                                                                                                                                                                                                                                                                                 |
|:-----------------------------------------------------------|--------------|---------------------|---------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SELECT` [`SelectExprList`](#selectexprlist)               | **``✓``**    | **``✓``**           | **``✓``**           | `SelectExprList` is a list that need to be projected from data source, generally including column names, expressions, or '*' for all columns.                                                                                                                                                                                                              |
| `FROM` [`TableRefs`](#tablerefs)                           | **``✓``**    | **``✓``**           | **``✓``**           | The From clause indicates the data source.<br />The data source can be one table (`select * from t;`) or multiple tables that JOIN together (`select * from t1 join t2;`) or no table ( `select 1+1;`), see [NO_TABLE SELECT](../dql/NO_TABLE_SELECT_CLAUSE.md)                                                                                            |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)                     | **``✓``**    | **``✓``**           | **``✓``**           | The Join clause indicates that the data source comes from multiple joined tables. OpenMLDB currently only supports LAST JOIN. During Online Serving, please follow [the specification of LAST JOIN under Online Serving](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md#online-servinglast-join)                                                      |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)                   | **``✓``**    | **``✓``**           |                     | The Where clause is used to set filter conditions, and only the data that meets the conditions will be included in the query result.                                                                                                                                                                                                                       |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md)             | **``✓``**    |                     |                     | The Group By clause is used to group the query results.The grouping conditions only support simple columns.                                                                                                                                                                                                                                                |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)                 | **``✓``**    |                     |                     | The Having clause is similar to the Where clause. The Having clause filters data after GroupBy, and the Where clause is used to filter records before aggregation.                                                                                                                                                                                         |                                                                                                                                                                                                                                                                                                                                                                              |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)                 | **``✓``**    |                     | **``✓``**           | The window clause is used to define one or several windows. Windows can be named or anonymous. Users can call aggregate functions on the window to perform analysis (```sql agg_func() over window_name```). During Online Serving, please follow [Online Serving下Window的使用规范](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.html#online-servingwindow) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)                   | **``✓``**    | **``✓``**           | **``✓``**           | The Limit clause is used to limit the number of results. OpenMLDB currently only supports one parameter to limit the maximum number of rows of returned data.                                                                                                                                                                                              |
| `ORDER BY` Clause                                          |              |                     |                     | Standard SQL also supports the OrderBy clause, however OpenMLDB does not support this clause currently. For example, the query `SELECT * from t1 ORDER BY col1;` is not supported in OpenMLDB.                                                                                                                                                             |

```{warning}
The `SELECT` running in online mode or the stand-alone version may not obtain complete data.
Because a query may perform a large number of scans on multiple tablets, for stability, the maximum scanned data on one tablet has been limited, namely `scan_max_bytes_size`.

If the select results are truncated, the message of `reach the max byte ...` will be recorded in the tablet's log, but there will not be an error.
```
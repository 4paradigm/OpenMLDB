# JOIN Clause

OpenMLDB currently only supports `LAST JOIN`.

LAST JOIN can be seen as a special kind of `LEFT JOIN`. On the premise that the JOIN condition is met, each row of the left table is joined with the last row of the right table that meets the condition. There are two types of `LAST JOIN`: unsorted join and sorted join.

- Unsorted join will join two tables directly without sorting the right table.
- Sorted join will sort the right table first, and then join two tables.

## Syntax

```
JoinClause
         ::= TableRef JoinType 'JOIN' TableRef [OrderClause] 'ON' Expression 
JoinType ::= 'LAST'       
```

## SQL Statement Template

```sql
SELECT ... FROM table_ref LAST JOIN tab le_ref;
```

## Boundary Description

| SELECT statement elements | state            | direction                                                         |
| :------------- | --------------- | :----------------------------------------------------------- |
| JOIN Clause    | Only LAST JOIN is supported | Indicates that the data source multiple tables JOIN. OpenMLDB currently only supports LAST JOIN. During Online Serving, you need to follow [The usage specification of LAST JOIN under Online Serving](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md#online-serving usage specification of last-join) |

### LAST JOIN without ORDER BY

#### Example: 
**LAST JOIN Unsorted Concatenation**

```sql
-- desc: simple spelling query without ORDER BY

SELECT t1.col1 as t1_col1, t2.col1 as t2_col2 from t1 LAST JOIN t2 ON t1.col1 = t2.col1
```

When `LAST JOIN` is spliced without sorting, the first hit data row is spliced

![Figure 7: last join without order](../dql/images/last_join_without_order.png)


Take the second row of the left table as an example, the right table that meets the conditions is unordered, there are 2 hit conditions, select the last one `5, b, 2020-05-20 10:11:12`

![Figure 8: last join without order result](../dql/images/last_join_without_order2.png)

The final result is shown in the figure above.

### LAST JOIN with ORDER BY

#### Example:
LAST JOIN Sorting And Splicing

```SQL
-- desc: Simple spelling query with ORDER BY
SELECT t1.col1 as t1_col1, t2.col1 as t2_col2 from t1 LAST JOIN t2 ORDER BY t2.std_ts ON t1.col1 = t2.col1
```

When `LAST JOIN` is configured with `Order By`, the right table is sorted by Order, and the last hit data row is spliced.

![Figure 9: last join with order](../dql/images/last_join_with_order1.png)

Taking the second row of the left table as an example, there are 2 items in the right table that meet the conditions. After sorting by `std_ts`, select the last item `3, b, 2020-05-20 10:11:13`

![Figure 10: last join with order result](../dql/images/last_join_with_order2.png)

The final result is shown in the figure above.

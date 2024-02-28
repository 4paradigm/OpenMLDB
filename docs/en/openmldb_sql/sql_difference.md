# Main Differences from Standard SQL

This article provides a comparison between the main usage of OpenMLDB SQL (SELECT query statements) and standard SQL (using MySQL-supported syntax as an example). It aims to help developers with SQL experience quickly adapt to OpenMLDB SQL.

Unless otherwise specified, the default version is OpenMLDB: >= v0.7.1

## Support Overview

The table below summarizes the differences in overall performance between OpenMLDB SQL and standard SQL based on SELECT statement elements across three execution modes (for execution mode details, please refer to [Workflow and Execution Modes](../quickstart/concepts/modes.md)). OpenMLDB SQL is currently partially compatible with standard SQL, with additional syntax introduced to accommodate specific business scenarios. New syntax is indicated in bold in the table.

Note: ✓ indicates that the statement is supported, while ✕ indicates that it is not.

|                | **OpenMLDB SQL**<br>**Offline Mode** | **OpenMLDB SQL**<br>**Online Preview Mode** | **OpenMLDB SQL**<br>**Online Request Mode** | **Standard SQL** | **Remarks**                                              |
| -------------- | ---------------------------- | -------------------------------- | -------------------------------- | ------------ | ------------------------------------------------------------ |
| WHERE Clause | ✓                            | ✓                                | ✕                                | ✓            | Some functionalities can be achieved through built-in functions with the `_where` suffix. |
| HAVING Clause | ✓                            | ✓                                | X                                | ✓            |                                                              |
| JOIN Clause | ✓                            | ✕                                | ✓                                | ✓            | OpenMLDB only supports **LAST JOIN** and **LEFT JOIN**. |
| GROUP BY  | ✓                            | ✕                                | ✕                                | ✓            |                                                              |
| ORDER BY  | ✓                           | ✓                                | ✓                               | ✓            | Support is limited to usage within the `WINDOW` and `LAST JOIN` clauses; it does not support reverse sorting in `DESC`. |
| LIMIT  | ✓                            | ✓                                | ✕                                | ✓            |                                                              |
| WINDOW Clause | ✓                            | ✓                                | ✓                                | ✓            | OpenMLDB includes new syntax **WINDOW UNION** and **WINDOW ATTRIBUTES**. |
| WITH Clause | ✕                            | ✕                                | ✕                                | ✓            | OpenMLDB supports begins from version v0.8.0. |
| Aggregate Function | ✓                            | ✓                                | ✓                                | ✓            | OpenMLDB has more extension functions. |



## Explanation of Differences

### Difference Dimension

Compared to standard SQL, the differences in OpenMLDB SQL can be explained from three main perspectives:

1. **Execution Mode**: OpenMLDB SQL has varying support for different SQL statements in three distinct execution modes: offline mode, online preview mode, and online request mode. The choice of execution mode depends on specific requirements. In general, for real-time computations in SQL, business SQL must adhere to the constraints of the online request mode.
2. **Clause Combinations**: The combination of different clauses can introduce additional limitations. In these scenarios, one clause operates on the result set of another clause. For example, when LIMIT is applied to WHERE, the SQL would resemble `SELECT * FROM (SELECT * FROM t1 WHERE id >= 2) LIMIT 2`. The term 'table reference' used here refers to `FROM TableRef`, which does not represent a subquery or a complex FROM clause involving JOIN or UNION.
3. **Special Restrictions**: Unique restrictions that do not fit the previous categories are explained separately. These restrictions are usually due to incomplete functionality or known program issues.

### Configuration of Scanning Limits

To prevent user errors from affecting online performance, OpenMLDB has introduced relevant parameters that limit the number of full table scans in offline mode and online preview mode. If these limitations are enabled, certain operations involving scans of multiple records (such as `SELECT *`, aggregation operations, etc.) may result in truncated results and, consequently, incorrect outcomes. It's essential to note that these parameters do not affect the accuracy of results in online request mode.

The configuration of these parameters is done within the tablet configuration file `conf/tablet.flags`, as detailed in the document on [Configuration File](../deploy/conf.md#the-configuration-file-for-tablet-conftabletflags). The parameters affecting scan limits include:

- Maximum Number of Scans: `--max_traverse_cnt`
- Maximum Number of Scanned Keys: `--max_traverse_pk_cnt`
- Size Limit for Returned Results: `--scan_max_bytes_size`

Starting from version 0.8.0, the parameter `max_traverse_pk_cnt` has been removed, and the parameter `max_traverse_key_cnt` should be used instead. The default values for the first two parameters are changed to 0, meaning there are no restrictions.

For version 0.8.4 and later (excluding), the `--scan_max_bytes_size` parameter also defaults to 0. It's important to note the configuration changes for these parameters in earlier versions.

### WHERE Clause

| **Apply To** | **Offline Mode** | **Online Preview Mode** | **Online Request Mode** |
| ------------------ | ------------ | ---------------- | ---------------- |
| Table References | ✓            | ✓                | ✕                |
| LAST JOIN          | ✓            | ✓                | ✕                |
| Subquery/ WITH Clause | ✓            | ✓                | ✕                |

In the online request mode, the `WHERE` clause isn't supported. However, some functionalities can be achieved through computation functions with the `_where` suffix, like `count_where` and `avg_where`, among others. For detailed information, please refer to [Built-In Functions](./udfs_8h.md).

### LIMIT Clause

LIMIT is followed by an INT literal, and it does not support other expressions. It indicates the maximum number of rows for returned data. However, LIMIT is not supported in the online mode.

| **Apply to**      | **Offline Mode** | **Online Preview Mode** | **Online Request Mode** |
| ----------------- | ---------------- | ----------------------- | ----------------------- |
| Table Reference   | ✓                | ✓                       | ✕                       |
| WHERE             | ✓                | ✓                       | ✕                       |
| WINDOW            | ✓                | ✓                       | ✕                       |
| LAST JOIN         | ✓                | ✓                       | ✕                       |
| GROUP BY & HAVING | ✕                | ✓                       | ✕                       |

### WINDOW Clause

The WINDOW clause and the GROUP BY & HAVING clause cannot be used simultaneously. When transitioning to the online mode, the input table for the WINDOW clause must be either a physical table or a simple column filtering, along with LAST JOIN concatenation of the physical table. Simple column filtering entails a select list containing only column references or renaming columns, without additional expressions. You can refer to the table below for specific support scenarios. If a scenario is not listed, it means that it's not supported.

| **Apply to**                                                 | **Offline Mode** | **Online Preview Mode** | **Online Request Mode** |
| ------------------------------------------------------------ | ---------------- | ----------------------- | ----------------------- |
| Table Reference                                              | ✓                | ✓                       | ✓                       |
| GROUP BY & HAVING                                            | ✕                | ✕                       | ✕                       |
| LAST JOIN                                                    | ✓                | ✓                       | ✓                       |
| Subqueries are only allowed under these conditions:<br/>1. Simple column filtering from a single table<br/>2. Multi-table LAST JOIN<br/>3. Simple column filtering after a dual-table LAST JOIN<br/> | ✓                | ✓                       | ✓                       |

Special Restrictions:

- In online request mode, the input for WINDOW can be a LAST JOIN or a LAST JOIN within a subquery. It's important to note that the columns for `PARTITION BY` and `ORDER BY` in the window definition must all originate from the leftmost table of the JOIN.

### GROUP BY & HAVING Clause

The GROUP BY statement is still considered an experimental feature and only supports a physical table as the input table. It's not supported in other scenarios. GROUP BY is also not available in the online mode.

| **Apply to**    | **Offline Mode** | **Online Preview Mode** | **Online Request Mode** |
| --------------- | ---------------- | ----------------------- | ----------------------- |
| Table Reference | ✓                | ✓                       | ✕                       |
| WHERE           | ✕                | ✕                       | ✕                       |
| LAST JOIN       | ✕                | ✕                       | ✕                       |
| Subquery        | ✕                | ✕                       | ✕                       |

### JOIN Clause

OpenMLDB exclusively supports the LAST JOIN and LEFT JOIN syntax. For a detailed description, please refer to the section on JOIN in the extended syntax. A JOIN consists of two inputs, the left and right. In the online request mode, it supports two inputs as physical tables or specific subqueries. You can refer to the table for specific details. If a scenario is not listed, it means it's not supported.

| **Apply to**                                     | **Offline Mode** | **Online Preview Mode** | **Online Request Mode** |
| ---------------------------------------------- | ------------ | ---------------- | ---------------- |
| LAST JOIN + two table reference                    | ✓            | ✕                | ✓                |
| LAST JOIN + simple column filtering for both tables| ✓            | ✕                | ✓                |
| LAST JOIN + left table is filtering with WHERE     | ✓            | ✕                | ✓                |
| LAST JOIN one of the table is WINDOW or LAST JOIN  | ✓            | ✕                | ✓                |
| LAST JOIN + right table is LEFT JOIN subquery      | ✕            | ✕                | ✓                |
| LEFT JOIN                                          | ✕            | ✕                | ✕                |

Special Restrictions:
- Launching LAST JOIN for specific subqueries involves additional requirements. For more information, please refer to [Online Requirements](../openmldb_sql/deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#specifications-of-last-join-under-online-request-mode).
- LAST JOIN and LEFT JOIN is currently not supported in online preview mode.

### WITH Clause

OpenMLDB (>= v0.7.2) supports non-recursive WITH clauses. The WITH clause functions equivalently to how other clauses work when applied to subqueries. To understand how the WITH statement is supported, please refer to its corresponding subquery writing methods as explained in the table above.

No special restrictions apply in this case.

### ORDER BY Keyword

The sorting keyword `ORDER BY` is only supported within the `WINDOW` and `LAST JOIN` clauses in the window definition, and the reverse sorting keyword `DESC` is not supported. Detailed guidance on these clauses can be found in the WINDOW and LAST JOIN sections.

### Aggregate Function

Aggregation functions can be applied to all tables or windows. Window aggregation queries are supported in all three modes. Full table aggregation queries are only supported in online preview mode and are not available in offline and online request modes.

- Regarding full table aggregation, OpenMLDB v0.6.0 began supporting this feature in online preview mode. However, it's essential to pay attention to the described [Scanning Limit Configuration](https://openmldb.feishu.cn/wiki/wikcnhBl4NsKcAX6BO9NDtKAxDf#doxcnLWICKzccMuPiWwdpVjSaIe).

- OpenMLDB offers various extensions for aggregation functions. To find the specific functions supported, please consult the product documentation in [OpenMLDB Built-In Function](../openmldb_sql/udfs_8h.md).

## Extended Syntax

OpenMLDB has focused on deep customization of the `WINDOW` and `LAST JOIN` statements and this section will provide an in-depth explanation of these two statements.

### WINDOW Clause

A typical WINDOW statement in OpenMLDB generally includes the following elements:

- Data Definition: Defines the data within the window using `PARTITION BY`.
- Data Sorting: Defines the data sorting within the window using `ORDER BY`.
- Scope Definition: Determines the direction of time extension through `PRECEDING`, `CURRENT ROW`, and `UNBOUNDED`.
- Range Unit: Utilizes `ROWS` and `ROWS_RANGE` to specify the unit of window sliding range.
- Window Attributes: Includes OpenMLDB-specific window attribute definitions, such as `MAXSIZE`, `EXCLUDE CURRENT_ROW`, `EXCLUDE CURRENT_TIME`, and `INSTANCE_NOT_IN_WINDOW`.
- Multi-table Definition: Uses the extended syntax `WINDOW ... UNION` to determine whether concatenation of cross-table data sources is required.

For a detailed syntax of the WINDOW statement, please refer to the [WINDOW Documentation](../openmldb_sql/dql/WINDOW_CLAUSE.md)

| **Statement Element**              | **Support Syntax**                                           | **Description**                                              | Required? |
| ---------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | --------- |
| Data Definition                    | PARTITION BY                                                 | OpenMLDB supports multiple column data types: bool, int16, int32, int64, string, date, timestamp. | ✓         |
| Data Sorting                       | ORDER BY                                                     | - It only supports sorting on a single column. <br />- Supported data types for sorting include int16, int32, int64, and timestamp. <br />- Reverse order (`DESC`) is not supported.  <br />- Must specify for versions before v0.8.4 | -         |
| Scope Definition                   | <be> Basic upper and lower bounds definition: ROWS/ROWS_RANGE BETWEEN ... AND ... <be> Scope definition is supported with keywords PRECEDING, OPEN PRECEDING, CURRENT ROW, UNBOUNDED              | - Must specify both upper and lower boundaries. <br />- The boundary keyword `FOLLOWING` is not supported. <br />- In online request mode, `CURRENT ROW` represents the present request line. From a table perspective, the current row is virtually inserted into the appropriate position in the table based on the `ORDER BY` criteria. | ✓         |
| Scope Unit                         | ROWS<br>ROWS_RANGE (Extended) | - ROW_RANGE is an extended syntax for defining window boundaries similar to standard SQL RANGE-type windows. It allows defining window boundaries with either numerical values or values with time units. This is an extended syntax.<br />- Window ranges defined in time units are equivalent to window definitions where time is converted into milliseconds. For example, `ROWS_RANGE 10s PRECEDING ...` and `ROWS_RANGE 10000 PRECEDING...` are equivalent. | ✓         |
| Window Properties (Extended)       | MAXSIZE <br>EXCLUDE CURRENT_ROW<br>EXCLUDE CURRENT_TIME<br>INSTANCE_NOT_IN_WINDOW | MAXSIZE is only valid to ROWS_RANGE <be> Without ORDER BY and EXCLUDE CURRENT_TIME cannot be used together        | -         |
| Multi Table Definition (Extension) | In practical use, the syntax form is relatively complex. Please refer to:<br/>[Cross Table Feature Development Tutorial](../tutorial/tutorial_sql_2.md)<br/>[WINDOW UNION Syntax Documentation](../openmldb_sql/dql/WINDOW_CLAUSE.md#1-window--union) | - Merging of multiple tables is allowed <br />- Union of simple subqueries is allowed <br />- It is commonly used in combination with aggregation functions for cross-table aggregation operations. | -         |
| Incognito Window                   | -                                                            | Complete window definition must include `PARTITION BY`, `ORDER BY`, and window range definition. | -         |

#### Special Restrictions

In online preview mode or offline mode, there are certain known issues when using LIMIT or WHERE clauses as inputs to the WINDOW clause, and it's generally not recommended.

#### Example of Window Definition

Define a `ROWS` type window with a range from the first 1000 rows to the current row.

```SQL
SELECT 
  sum(col2) OVER w1 as w1_col2_sum 
FROM 
  t1WINDOW w1 AS (
    PARTITION BY col1 
    ORDER BY 
      col5 ROWS BETWEEN 1000 PRECEDING 
      AND CURRENT ROW
  );
```

Define a `ROWS_RANGE` type window with a range covering all rows in the first 10 seconds of the current row, including the current row.

```SQL
SELECT 
  sum(col2) OVER w1 as w1_col2_sum 
FROM 
  t1WINDOW w1 AS (
    PARTITION BY col1 
    ORDER BY 
      col5 ROWS_RANGE BETWEEN 10s PRECEDING 
      AND CURRENT ROW
  );
```

Define a `ROWS` type window with a range from the first 1000 rows to the current row, containing only the current row and no other data at the current time.

```SQL
SELECT 
  sum(col2) OVER w1 as w1_col2_sum 
FROM 
  t1 WINDOW w1 AS (
    PARTITION BY col1 
    ORDER BY 
      col5 ROWS BETWEEN 1000 PRECEDING 
      AND CURRENT ROW EXCLUDE CURRENT_TIME
  );
```

Define a `ROWS_RANGE` type window with a range from the current time to the past 10 seconds, excluding the current request line.

```SQL
SELECT 
  sum(col2) OVER w1 as w1_col2_sum 
FROM 
  t1 WINDOW w1 AS (
    PARTITION BY col1 
    ORDER BY 
      col5 ROWS_RANGE BETWEEN 10s PRECEDING 
      AND CURRENT ROW EXCLUDE CURRENT_ROW
  );
```

Anonymous window:

```SQL
SELECT 
  id, 
  pk1, 
  col1, 
  std_ts, 
  sum(col1) OVER (
    PARTITION BY pk1 
    ORDER BY 
      std_ts ROWS BETWEEN 1 PRECEDING 
      AND CURRENT ROW
  ) as w1_col1_sumfrom t1;
```

#### Example of WINDOW ... UNION

In practical development, many applications store data in multiple tables. In such cases, the syntax `WINDOW ... UNION` is commonly used for cross-table aggregation operations. Please refer to the "Multi-Table Aggregation Features" section in the [Cross-Table Feature Development Tutorial](../tutorial/tutorial_sql_2.md).

### LAST JOIN Clause

For detailed syntax specifications for LAST JOIN, please refer to the [LAST JOIN Documentation](../openmldb_sql/dql/JOIN_CLAUSE.md#join-clause).

| **Statement Element** | **Support Syntax** | **Description**                                              | Required? |
| --------------------- | ------------------ | ------------------------------------------------------------ | --------- |
| ON                    | ✓                  | Supported column types include: BOOL, INT16, INT32, INT64, STRING, DATE, TIMESTAMP. | ✓         |
| USING                 | X                  | -                                                            | -         |
| ORDER BY              | ✓                  | - LAST JOIN extended syntax, not supported by LEFT JOIN. <br />- Only the following column types can be used: INT16, INT32, INT64, TIMESTAMP. <br />- The reverse order keyword DESC is not supported. | -         |

#### Example of LAST JOIN

```SQL
SELECT 
  * 
FROM 
  t1 
LAST JOIN t2 ON t1.col1 = t2.col1;

SELECT 
  * 
FROM 
  t1 
LEFT JOIN t2 ON t1.col1 = t2.col1;
```


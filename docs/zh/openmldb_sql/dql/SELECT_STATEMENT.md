# Query 语句

## Syntax Notation

- `[ expr ]`: 中括号，可选部分
- `{}`: 手动分组
- `a | b`: 逻辑或，表示 `a` 或 `b`
- `...`: 重复之前的部分, 重复次数 >= 0
- 大写变量，例如 `WITH`, 表示 SQL 关键词 "WITH"
- 小写变量, 例如 `query`, 可以拓展成特定语法结构

## Syntax

```yacc
query_statement:
  query [ CONFIG ( { key = value }[, ...] )]

query:
  [ WITH {non_recursive_cte}[, ...] ]
  { select | ( query ) | set_operation }
  [ ORDER BY ordering_expression ]
  [ LIMIT count ]

select:
  SELECT select_list
  [ FROM from_item ]
  [ WHERE bool_expression ]
  [ GROUP BY group_by_specification ]
  [ HAVING bool_expression ]
  [ window_clause ]

set_operation:
  query set_operator query

non_recursive_cte:
  cte_name AS ( query )

set_operator:
  UNION { ALL | DISTINCT }

from_item:
  table_name [ as_alias ]
  | { join_operation | ( join_operation ) }
  | ( query ) [ as_alias ]
  | cte_name [ as_alias ]

as_alias:
  [ AS ] alias_name

join_operation:
  condition_join_operation

condition_join_operation:
  from_item LEFT [ OUTER ] JOIN from_item join_condition
  | from_item LAST JOIN [ ORDER BY ordering_expression ] from_item join_condition

join_condition:
  ON bool_expression

window_clause:
  WINDOW named_window_expression [, ...]

named_window_expression:
  named_window AS { named_window | ( window_specification ) }

window_specification:
  [ UNION ( from_item [, ...] ) ]
  PARTITION BY expression [ ORDER BY ordering_expression ]
  window_frame_clause [ window_attr [, ...] ]

window_frame_clause:
  frame_units BETWEEN frame_bound AND frame_bound [ MAXSIZE numeric_expression ] )

frame_unit:
  ROWS 
  | ROWS_RANGE

frame_boud:
  { UNBOUNDED | numeric_expression | interval_expression } [ OPEN ] PRECEDING
  | CURRENT ROW

window_attr:
  EXCLUDE CURRENT_TIME
  | EXCLUDE CURRENT_ROW
  | INSTANCE_NOT_IN_WINDOW

// each item in select list is one of:
// - *
// - expression.*
// - expression
select_list:
  { select_all | select_expression } [, ...]

select_all:
  [ expression. ]*

select_expression:
  expression [ [ AS ] alias ]
```

## Query 语句元素

| SELECT语句元素                                     | 离线模式  | 在线预览模式 | 在线请求模式 | 说明                                                                                                                                                                                                                        |
|:-----------------------------------------------| --------- | ------------ | ------------ |:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`WITH` Clause](./WITH_CLAUSE.md) | **``✓``** | **``✓``**    | **``✓``**    |                                                                                                                                                                                           |
| [`SELECT` list](#selectexprlist) | **``✓``** | **``✓``**    | **``✓``**    | 投影操作列表，一般包括列名、表达式，或者是用 `*` 表示全部列                                                                                                                                                                                          |
| [`FROM` Clause](#from-clause)      | **``✓``** | **``✓``**    | **``✓``**    | 表示数据来源                                                   |
| [`JOIN` Operation](../dql/JOIN_CLAUSE.md) | **``✓``** | **``x``**    | **``✓``**    | 表示数据来源多个表JOIN。OpenMLDB目前仅支持LAST JOIN。在线请求模式下，需要遵循[Online Request下LAST JOIN的使用规范](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#在线请求模式下-last-join-的使用规范)                                                       |
| [`SET` Operation](./SET_OPERATION.md) | **``✓``** | **``✓``** | **``✓``** | 只支持 UNION, 在线支持 UNION ALL, 离线支持 UNION ALL/DISTINCT |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)       | **``x``** | **``✓``**    | **``x``** | Where 子句用于设置过滤条件，查询结果中只会包含满足条件的数据。                                                                                                                                                                                        |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md) | **``✓``** |  **``✓``**   | **``x``** | Group By 子句用于对查询结果集进行分组。分组表达式列表仅支持简单列。                                                                                                                                                                                    |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)     | **``✓``** |  **``✓``**   | **``x``** | Having 子句与 Where 子句作用类似.Having 子句过滤 GroupBy 后的各种数据，Where 子句在聚合前进行过滤。                                                                                                                                                      |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)     | **``✓``** |  **``✓``**   | **``✓``**    | 窗口子句用于定义一个或者若干个窗口。窗口可以是有名或者匿名的。用户可以在窗口上调用聚合函数来进行一些分析型计算的操作（```sql agg_func() over window_name```)。线请求模式下，需要遵循[Online Request下Window的使用规范](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#在线请求模式下window的使用规范) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)       | **``✓``** | **``✓``**    | **``x``** | Limit子句用于限制返回的结果条数。目前Limit仅支持接受一个参数，表示返回数据的最大行数。                                                                                                                                                                          |
| `ORDER BY` Clause                              | **``x``** | **``x``** | **``x``** | 标准SQL还支持Order By子句。OpenMLDB目前尚未支持Order子句。例如，查询语句`SELECT * from t1 ORDER BY col1;`在OpenMLDB中不被支持。                                                                                                                          |

```{warning}
在线模式或单机版的select，可能无法获取完整数据。单个tablet 限制了最大扫描数据量，即`scan_max_bytes_size`，默认为无限。但如果你配置了它，查询的数据量超过这个值，会出现结果截断。如果出现select结果截断，tablet 会出现`reach the max byte ...`的日志，但查询不会报错。

即使你没有配置`scan_max_bytes_size`，也可能出现select失败，比如 `body_size=xxx from xx:xxxx is too large`, ` Fail to parse response from xx:xxxx by baidu_std at client-side`等错误。我们不推荐全表扫描在线表，如果你想获得在线表的数据条数，可以使用`SELECT COUNT(*) FROM table_name`。
```

## FROM Clause

FROM子句指定了查询的原始数据来源,即我们要在哪些表中查找所需的数据。它也会定义这些数据源间的联系,比如是否通过某些字段建立连接查询,从而将原本分散在多个表中的数据整合在一起,形成一个单一的数据集以供查询操作使用. 

FROM 子句的来源可以是:

- 表名或者 Common Table Expression (CTE), 同名情况下 CTE 优先级更高
- JOIN 操作, OpenMLDB 支持 LEFT JOIN 和 LAST JOIN
- 任意子查询, 被括在括号中

## CONFIG 子句

`query_statement` 最后可以带可选的 CONFIG 子句，`CONFIG ( key_string = value_expr, ...)` 的形式，表示对当前 SQL 的额外配置信息，目前支持的选项有：

| key_string | value_expr 类型 | 说明 |
| ---------- | ---------  | ---- |
| execute_mode | string | 表示当前 SQL 的执行模式，可选值有 `online`, `request`, `offline`, 如果不填，默认值为系统变量 `execute_mode` 设置的值，可以通过 SQL `show variables` 查看。 |
| values | 任意合法的表达式 | 详见 [纯 SQL 在线请求模式查询](#sql-request-query-in-raw-sql) |

## SQL request query in raw SQL

OpenMLDB >= 0.9.0 支持在 query statement 中用 CONFIG 子句配置 SQL 的执行模式 (`execute_mode`) 和请求行信息(`values`). 即当 CONFIG `execute_mode = 'request'` 时，将首先读取 CONFIG 中 `values` 的值, 并解析成请求行，因此不需要在其他地方，例如 JAVA SDK 接口手动传入请求行。CONFIG `values` 支持的格式有两种：
1. 小括号 `()` 包括起来的表达式列表，表示一行数据。例如 `(1, "str", timestamp(1000) )` 
2. 中括号 `[]` 包括起来的 N 个小括号 `()` 列表， 表示 N 行数据。例如 `[ (1, "str", timestamp(1000)), (2, "foo", timestamp(5000)) ]`

小括号 `()` 包含的整个表达式表示单行请求行，它内部的每一个表达式类型必须和 SQL 请求表的 schema 严格一致。 第一种格式，表示单行的在线请求模式查询，查询结果为单行数据。第二中格式，表示 N 行的在线请求模式查询，查询结果为 N 行。

```sql
-- table t1 of schema ( id int, val string, ts timestamp )

-- 执行请求行为 (10, "foo", timestamp(4000)) 的在线请求模式 query
SELECT id, count (val) over (partition by id order by ts rows between 10 preceding and current row)
FROM t1
CONFIG (execute_mode = 'online', values = (10, "foo", timestamp (4000)))
```

## 离线同步模式 Query

设置`SET @@sync_job=true`后的 Query 语句，就是离线同步模式下的 Query。在这个状态下的 Query 会展示结果到CLI（不建议在SDK中使用这种模式，不会得到正常的ResultSet）。

原理：Query 执行完成后各worker通过HTTP发送结果到TaskManager，TaskManager收集各个结果分片并保存到本地文件系统中。结果收集完成后，再从本地文件系统读取，读取后删除本地缓存的结果。

```{attention}
离线同步模式 Query 仅用于展示，不保证结果完整。整个结果收集中可能出现文件写入失败，丢失HTTP包等问题，我们允许结果缺失。
```

### 相关配置参数

TaskManager配置`batch.job.result.max.wait.time`，在 Query job完成后，我们会等待所有结果被收集并保存在TaskManager所在主机的文件系统中，超过这一时间将结束等待，返回错误。如果认为整个收集结果的过程没有问题，仅仅是等待时间不够，可以调大这一配置项，单位为ms，默认为10min。

Batch配置(spark.default.conf):

- spark.openmldb.savejobresult.rowperpost: 为了防止HTTP传送过多数据，我们对数据进行切割，默认为16000行。如果单行数据量较大，可以调小该值。
- spark.openmldb.savejobresult.posttimeouts: HTTP传送数据的超时配置，共三个超时配置项，用`,`分隔，分别为`ConnectionRequestTimeout,ConnectTimeout,SocketTimeout`，默认为`10000,10000,10000`。如果出现HTTP传输超时，可调整这一参数。

### 重置

如果使用过程中出现错误，可能导致Result Id无法正确重置。所有Result Id都被虚假占用时，会出现错误"too much running jobs to save job result, reject this spark job"。这时可以通过HTTP请求TaskManager来重置，POST内容如下：

```
curl -H "Content-Type:application/json" http://0.0.0.0:9902/openmldb.taskmanager.TaskManagerServer/SaveJobResult -X POST -d '{"result_id":-1, "json_data": "reset"}'
```

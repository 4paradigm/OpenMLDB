# 与标准 SQL 的主要差异

本文将 OpenMLDB SQL 的主要使用方式（SELECT 查询语句）与标准 SQL （以 MySQL 支持的语法为例）进行比较，让有 SQL 使用经验的开发者快速上手 OpenMLDB SQL。

以下如无特殊说明，默认均为 OpenMLDB 版本：>= v0.7.1

## 支持总览

下表根据 SELECT 语句元素对 OpenMLDB SQL 在三种执行模式下（关于执行模式参考[使用流程和执行模式](../quickstart/concepts/modes.md)）与标准 SQL 整体性的差异做了汇总。OpenMLDB SQL 目前部分兼容标准 SQL，但考虑实际业务场景需求新增了部分语法，下表加粗部分为新增语法。

注：✓ 表示**支持**该语句，✕ 表示**不支持**。

|                | **OpenMLDB SQL**<br>**离线模式** | **OpenMLDB SQL**<br>**在线预览模式** | **OpenMLDB SQL**<br>**在线请求模式** | **标准 SQL** | **备注**                                                     |
| -------------- | ---------------------------- | -------------------------------- | -------------------------------- | ------------ | ------------------------------------------------------------ |
| WHERE 子句     | ✓                            | ✓                                | ✕                                | ✓            |                                                              |
| HAVING 子句    | ✓                            | ✓                                | X                                | ✓            |                                                              |
| JOIN 子句      | ✓                            | ✕                                | ✓                                | ✓            | OpenMLDB 仅支持特有的 **LAST JOIN**                          |
| GROUP BY 分组  | ✓                            | ✕                                | ✕                                | ✓            |                                                              |
| ORDER BY 排序  | ✕                            | ✕                                | ✕                                | ✓            |                                                              |
| LIMIT 限制行数 | ✓                            | ✓                                | ✕                                | ✓            |                                                              |
| WINDOW 子句    | ✓                            | ✓                                | ✓                                | ✓            | OpenMLDB 增加了特有的 **WINDOW ... UNION** 和 **WINDOW ATTRIBUTES** 语法 |
| WITH 子句      | ✕                            | ✕                                | ✕                                | ✓            | OpenMLDB 从版本 v0.7.2 开始支持                           |
| 聚合函数       | ✓                            | ✓                                | ✓                                | ✓            | OpenMLDB 有较多扩展函数                                      |



## 差异详解

### 差异维度

与标准 SQL 比较，OpenMLDB SQL 的差异性主要会从三个维度进行说明：

1. 执行模式：在三种不同执行模式下（离线模式，在线预览模式，在线请求模式），不同的 SQL 的支持程度不一样，需要分开考察。普遍的，为了最终可以让 SQL 实现实时计算，**最终需要上线的业务 SQL 需要符合在线请求模式的要求**。
2. 子句组合：不同子句的组合会带来额外的限制，下文描述形式 A 应用于 B，表示 A 子句在 B 子句的结果上运行。比如 LIMIT 应用于 WHERE，则其 SQL 类似为：`SELECT * FROM (SELECT * FROM t1 WHERE id >= 2) LIMIT 2`。下文中的“表引用”是指 `FROM TableRef`，不是 subquery 也不是带有 JOIN/UNION 的复杂 FROM 子句。
3. 特殊限制：不归于以上两类的特殊限制，会进行单独说明，一般是由于功能支持不完善或者程序已知问题所引起。

### 扫描限制的配置

为了避免用户误操作而影响到在线性能，OpenMLDB 有相关参数来限制在离线模式和在线预览模式下的全表扫描次数。如果打开了这些参数的限制，将会导致部分涉及到多个记录扫描的操作（比如 SELECT *, 聚合操作等）可能出现结果截断，而最终产生错误结果。注意，这些参数不会影响到在线请求模式的结果正确性。

相关参数会在 tablet 配置文件 conf/tablet.flags 里进行配置，详见文档[配置文件](../deploy/conf.md#tablet配置文件-conftabletflags) 。影响到扫描限制的参数为：

- 最大扫描条数 `--max_traverse_cnt`
- 最大扫描 key 的个数 `--max_traverse_pk_cnt`
- 返回的结果大小限制 `--scan_max_bytes_size`

预计在 v0.7.3 以及以后版本中，以上参数的默认值都为 0，即不做相关限制。之前的版本需要注意相关参数的设置。

### WHERE 子句

| **应用于**   | **离线模式** | **在线预览模式** | **在线请求模式** |
| ------------------ | ------------ | ---------------- | ---------------- |
| 表引用             | ✓            | ✓                | ✕                |
| LAST JOIN          | ✓            | ✓                | ✕                |
| 子查询 / WITH 子句 | ✓            | ✓                | ✕                |

### LIMIT 子句

后面跟一个 INT literal, 不支持其它表达式，表示返回数据的最大行数。LIMIT 不支持上线。

| **应用于**        | **离线模式** | **在线预览模式** | **在线请求模式** |
| ----------------- | ------------ | ---------------- | ---------------- |
| 表引用            | ✓            | ✓                | ✕                |
| WHERE             | ✓            | ✓                | ✕                |
| WINDOW            | ✓            | ✓                | ✕                |
| LAST JOIN         | ✓            | ✓                | ✕                |
| GROUP BY & HAVING | ✕            | ✓                | X                |

### WINDOW 子句

WINDOW 子句和 GROUP BY & HAVING 子句不支持同时使用。上线时 WINDOW 的输入表必须是物理表或者对物理表的简单列筛选和 LAST JOIN 拼接（简单列筛选为 select list 只有列引用或者列的重命名，没有其它表达式），具体支持参照下表，未列出情况均为不支持。

| **应用于**                                                   | **离线模式** | **在线预览模式** | **在线请求模式** |
| ------------------------------------------------------------ | ------------ | ---------------- | ---------------- |
| 表引用                                                       | ✓            | ✓                | ✓                |
| GROUP BY & HAVING                                            | ✕            | ✕                | ✕                |
| LAST JOIN                                                    | ✓            | ✓                | ✓                |
| 子查询，仅以下情况：<br>1. 单表简单列筛选<br>2.多表 LAST JOIN<br> 3.双表 LAST JOIN 后的简单列筛选 | ✓            | ✓                | ✓                |

特殊限制：

- 在线请求模式下，WINDOW 的输入是 LAST JOIN 或者子查询内的 LAST JOIN, 注意窗口的定义里 `PARTITION BY` & `ORDER BY` 的列都必须来自 JOIN 最左边的表。

### GROUP BY & HAVING 子句

GROUP BY 语句，目前仍为实验性功能，仅支持输入表是一张物理表， 其它情况不支持。GROUP BY 不支持上线。

| **应用于** | **离线模式** | **在线预览模式** | **在线请求模式** |
| ---------- | ------------ | ---------------- | ---------------- |
| 表引用     | ✓            | ✓                | ✕                |
| WHERE      | ✕            | ✕                | ✕                |
| LAST JOIN  | ✕            | ✕                | ✕                |
| 子查询     | ✕            | ✕                | ✕                |

### JOIN 子句（LAST JOIN）

OpenMLDB 仅支持 LAST JOIN 一种 JOIN 语法，详细描述参考扩展语法的 LAST JOIN 部分。JOIN 有左右两个输入，在线请求模式下，支持两个输入为物理表，或者特定的子查询，详见表格，未列出情况不支持。

| **应用于**                                                   | **离线模式** | **在线预览模式** | **在线请求模式** |
| ------------------------------------------------------------ | ------------ | ---------------- | ---------------- |
| 两个表引用                                                   | ✓            | ✕                | ✓                |
| 子查询, 仅包括：<br>左右表均为简单列筛选<br>左右表为 WINDOW 或 LAST JOIN 操作 | ✓            | ✓                | ✓                |

特殊限制：

- 关于特定子查询的 LAST JOIN 上线，还有额外要求，详见[上线要求](../openmldb_sql/deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#在线请求模式下-last-join-的使用规范) 。
- 在线预览模式下不支持 LAST JOIN, 见 issue： https://github.com/4paradigm/OpenMLDB/issues/2976

### WITH 子句

OpenMLDB (>= v0.7.2) 支持非递归的 WITH 子句。WITH 子句等价于其它子句应用于子查询时的语义，WITH 语句的支持情况，可参照其对应子查询写法下，上述表格的说明。

特殊限制：

- 无

### 聚合函数

聚合函数可应用于全表或窗口。三种模式下均支持窗口聚合查询；全表聚合查询仅在在线预览模式下支持，离线和在线请求模式不支持。

特殊限制：

- OpenMLDB v0.6.0 开始支持在线预览模式的全表聚合，但注意所描述的[扫描限制配置](https://openmldb.feishu.cn/wiki/wikcnhBl4NsKcAX6BO9NDtKAxDf#doxcnLWICKzccMuPiWwdpVjSaIe)。
- OpenMLDB 有自己的聚合函数列表，请查看产品文档具体查询所支持的函数 [OpenMLDB 内置函数](../openmldb_sql/functions_and_operators/Files/udfs_8h.md)。

## 扩展语法

OpenMLDB 主要对 WINDOW 以及 LAST JOIN 语句进行了深度定制化开发，本节将对这两个语法进行详细说明。

### WINDOW 子句

在 OpenMLDB 里使用到的一个典型的 WINDOW 语句一般会包含以下元素：

- 数据定义：通过 `PARTITION BY` 定义窗口内数据
- 数据排序：通过 `ORDER BY` 定义窗口内数据排序
- 范围定义：通过 `PRECEDING`, `CURRENT ROW`, `UNBOUNDED` 定义时间延展方向
- 范围单位：通过 `ROWS`, `ROWS_RANGE`  定义窗口滑动范围的单位
- 窗口属性：OpenMLDB 特有的窗口属性定义，包括 `MAXSIZE`, `EXECLUDE CURRENT_ROW`, `EXCLUDE CURRENT_TIME`, `INSTANCE_NOT_IN_WINDOW`
- 多表定义：通过扩展语法 `WINDOW ... UNION` 定义是否需要进行跨表数据源拼接

关于 WINDOW 详细语法，请参考 [WINDOW 文档](../openmldb_sql/dql/WINDOW_CLAUSE.md)。

| **语句元素**     | **支持语法**                                                 | **说明**                                                     | **必需 ？** |
| ---------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------- |
| 数据定义         | PARTITION BY                                                 | 可支持多列<br>支持的列数据类型: bool, int16, int32, int64, string, date, timestamp | ✓           |
| 数据排序         | ORDER BY                                                     | 仅支持对单一列排序<br>可支持数据类型: int16, int32, int64, timestamp<br>不支持倒序 `DESC` | ✓           |
| 范围定义         | <br>基本上下界定义语法：ROWS/ROWS_RANGE BETWEEN ... AND ...<br>支持范围定义关键字 PRECEDING, OPEN PRECEDING, CURRENT ROW, UNBOUNDED | 必须给定上下边界<br>不支持边界关键字 FOLLOWING<br>在线请求模式中，CURRENT ROW 为当前的请求行。在表格视角下，当前行将会被虚拟的插入到表格根据 ORDER BY 排序的正确位置上。 | ✓           |
| 范围单位         | ROWS<br>ROWS_RANGE（扩展）                                       | ROWS_RANGE 为扩展语法，其定义的窗口边界属性等价于标准 SQL 的 RANGE 类型窗口，支持用数值或者带时间单位的数值定义窗口边界，后者为拓展语法。<br>带时间单位定义的窗口范围，等价于时间转化成毫秒数值后的窗口定义。例如 `ROWS_RANGE 10s PRCEDING ...` 和 `ROWS_RANGE 10000 PRECEDNG ...` 是等价的。 | ✓           |
| 窗口属性（扩展） | MAXSIZE <br>EXCLUDE CURRENT_ROW<br>EXCLUDE CURRENT_TIME<br>INSTANCE_NOT_IN_WINDOW | MAXSIZE  只对 ROWS_RANGE 有效                                | -           |
| 多表定义（扩展） | 实际使用中语法形态较为复杂，参考：<br>[跨表特征开发教程](../tutorial/tutorial_sql_2.md)<br>[WINDOW UNION 语法文档](../openmldb_sql/dql/WINDOW_CLAUSE.md#1-window--union) | 允许合并多个表<br>允许联合简单子查询<br>实践中，一般和聚合函数搭配使用，实现跨表的聚合操作 | -           |
| 匿名窗口         | -                                                            | 必须包括 PARTITION BY、ORDER BY、以及窗口范围定义            | -           |

#### 特殊限制

- 在线预览模式或者离线模式下，WINDOW 的输入是 LIMIT 或者 WHERE 子句，得到的计算结果可能不正确, 不建议使用。

#### 窗口定义举例

定义 ROWS 类型窗口，窗口范围是前 1000 行到当前行：

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

定义 ROWS_RANGE 类型窗口，窗口范围是当前行前 10 秒的所有行，以及当前行：

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

定义一个 ROWS 类型窗口，窗口范围是前1000行到当前行。 除了当前行以外窗口内不包含当前时刻的其他数据：

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

定义一个 ROWS_RANGE 类型窗口，窗口范围为当前时间到过去 10 秒，但是不包含当前请求行：

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

匿名窗口：

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

#### WINDOW ... UNION 举例

在实际开发中，较多的应用的数据是存放在多个表格中，在这种情况下，一般会使用 WINDOW ... UNION 的语法进行跨表的聚合操作。请参考[跨表特征开发教程](../tutorial/tutorial_sql_2.md)。

### LAST JOIN 子句

关于 LAST JOIN 详细语法规范，请参考 [LAST JOIN 文档](../openmldb_sql/dql/JOIN_CLAUSE.md#join-clause)。

| **语句元素** | **支持语法** | **说明**                                                     | **必需？** |
| ------------ | ------------ | ------------------------------------------------------------ | ---------- |
| ON           | ✓            | 列类型支持：BOOL, INT16, INT32, INT64, STRING, DATE, TIMESTAMP | ✓          |
| USING        | 不支持       | -                                                            | -          |
| ORDER BY     | ✓            | 后面只能接单列列类型 : INT16, INT32, INT64, TIMESTAMP<br>不支持倒序关键字 DESC | -          |

#### LAST JOIN 举例

```SQL
SELECT 
  * 
FROM 
  t1 
LAST JOIN t2 ON t1.col1 = t2.col1;
```

# Changelog

## [0.4.0] - 2022 - 01 - 14

### Highlight

- The SQL-centric feature is enhanced for both standalone and cluster deployments. We have introduced the `task manager` and other modules to make SQL-centric development and deployment available for both standalone and cluster deployments. (#991,1034 @tobegit3hub)
- The Chinese documentations are thoroughly polished and accessible at https://docs.openmldb.ai/ . This documentation repository is available at https://github.com/4paradigm/openmldb-docs-zh , and you are welcome to make contributions.
- Experimental feature: We have introduced a monitoring module based on Prometheus + Grafana for online feature processing. (#1048 @aceforeverd)

### Other Features

- Support SQL syntax: LIKE, HAVING
- Support new built-in functions: reverse (#1004 @nautaa), dayofyear (#856 @Nicholas-SR)
- Improve the compilation and install process, and support building from sources (#999,#871 @aceforeverd; #992 @vagetablechicken)
- Improve the GitHub CI/CD workflow
- Support system databases and tables
- Improve the function `create index`
- Improve the demo image (#1023 @zhanghaohit)
- Improve the Python SDK
- Simplify the concepts of execution modes
- Support data import and export for the cluster deployment
- Support default values when creating a table (#563 @zoyopei)
- Support string delimiters and quotes (#668 @ZackeryWang)
- Add a new `lru_cache` to support upsert (#795 vagetablechicken)
- Support adding index with any `ts_col` (#828 @dl239)
- Improve documentations (#952 #885 @mahengyang; #834 @Nicholas-SR)

### Bug Fixes

Small bug fixes

#847, #831, #647, #934, #953, #1015, #982, #927, #994, #1008, #1028, #1019, #779, #855, #350

@nautaa, @Nicholas-SR, @aceforeverd, @dl239, @jingchen2222, @tobegit3hub, @keyu813



## [0.3.0] - 2021 - 11 - 05

### Highlight
We introduce a new standalone mode that can be deployed on a single node, which is suitable for small businesses and the demonstration purpose. Please read more details from [here](https://github.com/4paradigm/OpenMLDB/blob/v0.3.0/docs/en/standalone.md). The standalone mode is particularly enhanced for ease of use based on the following features that are supported by standalone mode only.
* The standalone deployment mode https://github.com/4paradigm/OpenMLDB/issues/440
* Connection establishment by specifying the host name and port in CLI https://github.com/4paradigm/OpenMLDB/issues/441
* LOAD DATA command for bulk loading https://github.com/4paradigm/OpenMLDB/issues/443
* SQL syntax support for exporting data: SELECT INTO FILE https://github.com/4paradigm/OpenMLDB/issues/455
* Deployment commands: DEPLOY, SHOW DEPLOYMENT, and DROP DEPLOYMENT https://github.com/4paradigm/OpenMLDB/issues/460 https://github.com/4paradigm/OpenMLDB/issues/447
### Other Features
* A new CLI command to support different levels of performance sensitivity:  `SET performance_sensitive=true|false`. When it is set to false, SQL queries can be executed without indexes. Please read [here](https://github.com/4paradigm/OpenMLDB/blob/v0.3.0/docs/en/performance_sensitive_mode.md) for more details about the performance sensitivity configuration https://github.com/4paradigm/OpenMLDB/issues/555
* Supporting SQL queries over multiple databases https://github.com/4paradigm/OpenMLDB/issues/476
* Supporting inserting multiple tuples into a table using a single SQL https://github.com/4paradigm/OpenMLDB/issues/398
* Improvements for Java SDK:
The new API getTableSchema https://github.com/4paradigm/OpenMLDB/pull/483
The new API genDDL, which is used to generate DDLs according to a given SQL script https://github.com/4paradigm/OpenMLDB/issues/588
### Bugfix
* Exceptions caused by certain physical plans with special structures when performing column resolve for logical plans. https://github.com/4paradigm/OpenMLDB/issues/437
* Under specific circumstances, unexpected outcomes produced by SQL queries with the WHERE when certain WHERE conditions do not fit into indexes https://github.com/4paradigm/OpenMLDB/issues/599
* The bug when enabling WindowParallelOpt and WindowSkewOptimization at the same times https://github.com/4paradigm/OpenMLDB/issues/444
* The bug of LCA (Lowest Common Ancestor) algorithm to support WindowParallelOpt for particular SQLs https://github.com/4paradigm/OpenMLDB/issues/485
* Workaround for the Spark bug (SPARK-36932) when the columns with the same names in LastJoin https://github.com/4paradigm/OpenMLDB/issues/484
### Acknowledgement
We appreciate the contribution to this release from external contributors who are not from 4Paradigm's core OpenMLDB team, including [Kanekanekane](https://github.com/Kanekanekane), [shawn-happy](https://github.com/shawn-happy), [lotabout](https://github.com/lotabout), [Shouren](https://github.com/Shouren), [zoyopei](https://github.com/zoyopei), [huqianshan](https://github.com/huqianshan)

## [Unreleased]

### Feature
- Support insert multiple rows into a table using a single SQL insert statement. [#398](https://github.com/4paradigm/OpenMLDB/issues/398)
- Support aggregation function, e.g. `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, over the whole table [#219](https://github.com/4paradigm/OpenMLDB/issues/219)
- Enhance plan optimization on `GROUP` and `FILTER` op [#350](https://github.com/4paradigm/OpenMLDB/pull/350)
- Refactor status code and status macro. Save first message (root message) in `status.msg`. [#430](https://github.com/4paradigm/OpenMLDB/issues/430)

### Bug Fix
- Fix plan error triggered by optimize the same plan node repeatedly. [#437](https://github.com/4paradigm/OpenMLDB/issues/437)

## [0.2.3] - 2021-08-31
### Feature
- Data importer support bulk load [#250](https://github.com/4paradigm/OpenMLDB/pull/250)
- Support parameterized query under BatchMode [#262](https://github.com/4paradigm/OpenMLDB/issues/262), [#168](https://github.com/4paradigm/OpenMLDB/issues/168)
- Support Hive metastore and Iceberg tables for offline [#245](https://github.com/4paradigm/OpenMLDB/pull/245), [#146](https://github.com/4paradigm/OpenMLDB/pull/146)
- Integrated with Trino [#254](https://github.com/4paradigm/OpenMLDB/pull/254)
- Support global SortBy node for offline [#296](https://github.com/4paradigm/OpenMLDB/pull/296)

### Bug Fix
- Fix end2end offline tests for the same SQL [#300](https://github.com/4paradigm/OpenMLDB/pull/300)
- `desc` do not display the value of ttl after adding index[#156](https://github.com/4paradigm/OpenMLDB/issues/156)

### SQL Syntax
- `nvl` & `nvl2`: [#238](https://github.com/4paradigm/OpenMLDB/issues/238)
- bitwise operators: `&`, `|`, `^`, `~` [#244](https://github.com/4paradigm/OpenMLDB/pull/244)
- between predicate: [#277](https://github.com/4paradigm/OpenMLDB/pull/277)

## [0.2.2] - 2021-08-08
### Feature
+ Add `VARCHAR` Type [#237](https://github.com/4paradigm/OpenMLDB/issues/237)

### Bug Fix
- Fix invalid back qoute identifier name [#263](https://github.com/4paradigm/OpenMLDB/issues/263)
  can't write as multiple path style (e.g a.b) now
- InsertPreparedStatement set month by mistake when use Date type [#200](https://github.com/4paradigm/OpenMLDB/pull/200)

### Note:
`OPTIONS` can't write as multiple path style (e.g a.b) now

## [0.2.0] - 2021-07-22
### Features

+ Refactor front-end using [zetasql](https://github.com/jingchen2222/zetasql). Thus OpenMLDB can support more SQL syntaxs and provide friendly syntax error message.
+ Better code style and comment
+ Add APIServer module. User can use Rest API access OpenMLDB.[#48](https://github.com/4paradigm/OpenMLDB/issues/48)

### SQL Syntax

Changed
- `table options` syntax: [#103](https://github.com/4paradigm/HybridSE/issues/103)
- `lead` method: [#136](https://github.com/4paradigm/HybridSE/pull/136)

Removed
- `||` and `&&` as logical operator: [#264](https://github.com/4paradigm/OpenMLDB/issues/264)
- `at` function: [#265](https://github.com/4paradigm/OpenMLDB/issues/265)

### Note
- openmldb-0.2.0-linux.tar.gz targets on x86_64
- aarch64 artifacts consider experimental

[Unreleased]: https://github.com/4paradigm/OpenMLDB/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/4paradigm/OpenMLDB/compare/0.2.2...v0.2.3
[0.2.2]: https://github.com/4paradigm/OpenMLDB/compare/v0.2.0...0.2.2
[0.2.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.1.5-pre...v0.2.0

# Changelog

## [0.4.4] - 2022-04-01

### Features
- Support the standalone version by Java and Python SDKs (#1302 #1325 #1485 @tobegit3hub @HuilinWu2 @keyu813)
- Support the blocking execution for offline queries (#1486 @vagetablechicken )
- Add the `getStatement` API in Java SDK (#1231 @dl239 )
- Support multiple rows insertion in the Python SDK (#1402 @hezhaozhao-git )
- Support the JDBC connection (#1511 @vagetablechicken )

### Bug Fixes
- The error message is empty when executing `show deployment` in CLI fails. (#1415 @dl239 )
- The `show job` and `show jobs` cannot display correct information. (#1440 @vagetablechicken )
- The built-in function execution on a string filed with the length of greater than 2048 characters causes OpenMLDB crash. (#1540 @dl239 )
- The simple expression inference fails in some cases (#1443 @jingchen2222 )
- The `PreparedStatement` in Java SDK does not perform as expected.  (#1511 @vagetablechicken )

### Code Refactoring
#1467 @aimanfatima ; #1513 @L-Y-L ; #1503 @Stevinson ;

## [0.4.3] - 2022-03-15

### Features

- Add the output of the number of rows imported after successfully importing data (#1401 @Manny-op)
- Code Refactoring (#1366 @Cupid0320; #1378 @wuteek; #1418 @prashantpaidi; #1420 @shiyoubun; #1422 @vagetablechicken)

### Bug Fixes
- Loading online data with "not null" columns in Spark fails. (#1341 @vagetablechicken)
- `max_where` and `min_where` results are incorrect if there is no rows matched. (#1403 @aceforeverd)
- The `insert` and `select` execution of the standalone version fails. (#1426 @dl239)
- Other minor bug fixes (#1379 @wuteek; #1384 jasleon)

## [0.4.2] - 2022-03-01

### Features
- Support timestamps in `long int` when importing a csv file (#1237 @vagetablechicken)
- Change the default execution mode in CLI from `online` to `offline` (#1332 @dl239)
- Enhancements for the Python SDK:
  - Support `fetchmany` and `fetchall` in Python SDK (#1215 @HuilinWu2)
  - Support fetching logs of TaskManager jobs in Python SDK and APIs (#1214 @tobegit3hub)
  - Support fetching the schema of result sets in Python SDK (#1194 @tobegit3hub)
  - Support the SQL magic function in Jupyter Notebook when using the Python SDK. (#1164 @HuilinWu2)
- Enhancements for the TaskManager:
  - Taskmanager can find the local batchjob jar if the path is not configured. (#1250 @tobegit3hub)
  - Support the Yarn-client mode in TaskManager (#1265 @tobegit3hub)
  - Support correctness checking for TaskManager's configuration (#1262 @tobegit3hub)
  - Support reordering for the task list (#1256 @tobegit3hub)
- Add new UDF functions of `lower` and `lcase` (#1192 @Liu-2001)
- Offline queries that do not execute on tables will run successfully even when the connection fails. (#1264 @tobegit3hub) 

### Bug Fixes
- Offline data import fails when the timestamp value is `null`. (#1274 @tobegit3hub)
- Start time of TaskManager jobs in CLI is null. (#1272 @tobegit3hub)
- LAST JOIN may fail in the cluster version under certain circumstances. (#1226 @dl239)
- Invalid SQL may run successfully. (#1208 @aceforeverd)

## [0.4.1] - 2022-02-09

### Features
- Improve CLI error messages and support the 'enable_trace' system variable (#1129 @jingchen2222)

### Bug Fixes
- CLI coredumps when it fails to connect to a nameserver. (#1166 @keyu813)
- Java SDK has the issue of memory leaks. (#1148 @dl239)
- The startup fails if a pid file exists. (#1108 @dl239)
- There are incorrect values for the column with the date type when loading data into an online table. (#1103 @yabg-shuai666)
- Offline data import for the CSV format may cause incorrect results. (#1100 @yabg-shuai666)
- 'Offline path' cannot be displayed after importing offline data. (#1172 @vagetablechicken)

## [0.4.0] - 2022-01-14

### Highlights

- The SQL-centric feature is enhanced for both standalone and cluster versions. Now you can enjoy the SQL-centric development and deployment experience seamlessly. (#991,#1034,#1071,#1064,#1061,#1049,#1045,#1038,#1034,#1029,#997,#996,#968,#946,#840,#830,#814,#776,#774,#764,#747,#740,#466,#481,#1033,#1027,#966,#951,#950,#932,#853,#835,#804,#800,#596,#595,#568,#873,#1025,#1021,#1019,#994,#991,#987,#912,#896,#894,#893,#873,#778,#777,#745,#737,#701,#570,#559,#558,#553 @tobegit3hub; #1030,#965,#933,#920,#829,#783,#754,#1005,#998 @vagetablechicken)
- The Chinese documentations are thoroughly polished and accessible at https://docs.openmldb.ai/ . This documentation repository is available at https://github.com/4paradigm/openmldb-docs-zh , and you are welcome to make contributions.
- Experimental feature: We have introduced a monitoring module based on Prometheus + Grafana for online feature processing. (#1048 @aceforeverd)

### Other Features

- Support SQL syntax: LIKE, HAVING (#841 @aceforeverd; #927,#698 @jingchen2222)
- Support new built-in functions: reverse (#1004 @nautaa), dayofyear (#856 @Nicholas-SR)
- Improve the compilation and install process, and support building from sources (#999,#871,#594,#752,#793,#805,#875,#871,#999 @aceforeverd; #992 @vagetablechicken)
- Improve the GitHub CI/CD workflow (#842,#884,#875,#919,#1056,#874 @aceforeverd)
- Support system databases and tables (#773 @dl239)
- Improve the function `create index` (#828 @dl239)
- Improve the demo image (#1023,#690,#734,#751 @zhanghaohit)
- Improve the Python SDK (#913,#906 @tobegit3hub;#949,#909 @HuilinWu2; #838 @dl239)
- Simplify the concepts of execution modes (#877,#985,#892 @jingchen2222)
- Add data import and export for the cluster version (#1078 @tobegit3hub)
- Add new deployment command for the cluster version (#921 @dl239)
- Support default values when creating a table (#563 @zoyopei)
- Support string delimiters and quotes (#668 @ZackeryWang)
- Add a new `lru_cache` to support upsert (#795 @vagetablechicken)
- Support adding index with any `ts_col` (#828 @dl239)
- Improve the `ts` packing in `sql_insert_now` (#944 ,#974 @keyu813)
- Improve documentations (#952 #885 @mahengyang; #834 @Nicholas-SR; #792,#1058,#1002,#872,#836,#792 @lumianph; #844,#782 @jingchen2222; #1022,#805 @aceforeverd)
- Other minor updates (#1073 @dl239)

### Bug Fixes

#847, #831, #647, #934, #953, #1015, #982, #927, #994, #1008, #1028, #1019, #779, #855, #350, #631, #1074, #1073, #1081

@nautaa, @Nicholas-SR, @aceforeverd, @dl239, @jingchen2222, @tobegit3hub, @keyu813

## [0.3.0] - 2021-11-05

### Highlights
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

[0.4.4]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.3...v0.4.4
[0.4.3]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/4paradigm/OpenMLDB/compare/0.2.2...v0.2.3
[0.2.2]: https://github.com/4paradigm/OpenMLDB/compare/v0.2.0...0.2.2
[0.2.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.1.5-pre...v0.2.0

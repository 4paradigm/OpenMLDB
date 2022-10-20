# Changelog

## [0.6.4] - 2022-10-21

### Features
- Support new build-in functions like `top_n_value_*_cate_where` (#2622 @aceforeverd)
- Support online batch computation and full table aggregation (#2620 @zhanghaohit)
- Support load_mode and thread option for `LOAD DATA` (#2684 @zhanghaohit)
- Improve the documents (#2476, #2486 #2514 #2611 #2693 #2462 @michelle-qinqin, #2695 @lumianph)
- Run macos compiling job in cicd workflow (#2665 @dl239)

### Bug Fixes
- Fail to recreate the index that has been dropped previously (#2440 @dl239)
- `Traverse` method may get duplicate data if there are same ts records on one pk (#2637 @dl239)
- Window union will compile failed in batch mode (#2478 @tobegit3hub, #2561 @aceforeverd)
- `select * ...` statement may result inconsistent output schema in many cases (#2660 @aceforeverd)
- Result is incorrect if the window is specified as `UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT_ROW` (#2674 @aceforeverd)
- Incorrect slice offset may lead to offline jobs halt (#2687 @aceforeverd)
- Other minor bug fixes (#2669 dl239, #2683 @zhanghaohit)

### Code Refactoring
#2541 @dl239, #2573 #2672 @haseeb-xd


## [0.6.3] - 2022-10-14

### Features
- Support setting the configuration of `glog` for clients (#2482 @vagetablechicken)
- Add the checksum of SHA256 for release packages (#2560 @team-317)
- Support the new build-in function `unhex` (#2431 @aucker)
- Support the readable date and time format in CLI (#2568 @dl239)
- Support the `LAST JOIN` with a subquery as a producer of window node in the request mode (#2569 @aceforeverd)
- Upgrade the Spark version to 3.2.1 (#2566 @tobegit3hub, #2635 @dl239)
- Support setting the SQL cache size in SDKs (#2605 @vagetablechicken)
- Add a new interface of `ValidateSQL` to validate the syntax of SQL (#2626 @vagetablechicken)
- Improve the documents (#2405 #2492 #2562 #2496 #2495 #2436 #2487 #2623 @michelle-qinqin, #2543 @linjing-lab, #2584 @JourneyGo, #2567 #2583 @vagetablechicken, #2643 @dl239)
- Other minor features (#2504 #2572 #2498 #2598 @aceforeverd, #2555 #2641 @tobegit3hub, #2550 @zhanghaohit, #2595 @Elliezza, #2592 @vagetablechicken)

### Bug Fixes
- After a nameserver restarting, deployments may not recover. (#2533 @dl239)
- If the type of first column is `bool`, it fails to resolve the function `count_where`.  (#2570 @aceforeverd)
- Other minor bug fixes (#2540 #2577 #2625 #2655 @dl239, #2585 @snehalsenapati23, #2539 @vagetablechicken)

### Code Refactoring
#2516 #2520 #2522 #2521 #2542 #2531 #2581 @haseeb-xd, #2525 #2526 #2527 #2528 @kstrifonoff, #2523 @ighmaZ, #2546 #2549 @NevilleMthw, #2559 @marandabui, #2554 @gokullan, #2580 @team-317, #2599 @lbartyczak, #2594 @shivamgupta-sg, #2571 @Jake-00

## [0.6.2] - 2022-09-20

### Features
- Support independently executing the OpenMLDB offline engine without the OpenMLDB deployment (#2423 @tobegit3hub)
- Support the log setting of ZooKeeper and disable ZooKeeper logs in the diagnostic tool (#2451 @vagetablechicken)
- Support query parameters of the SQL query APIs (#2277 @qsliu2017)
- Improve the documents (#2406 @aceforeverd, #2408 #2414 @vagetablechicken, #2410 #2402 #2356 #2374 #2396 #2376 #2419 @michelle-qinqin, #2424 #2418 @dl239, #2455 @lumianph, #2458 @tobegit3hub)
- Other minor features (#2420 @aceforeverd, #2411 @wuyou10206, #2446 #2452 @vagetablechicken, #2475 @tobegit3hub)

### Bug Fixes
- Table creation succeeds even if `partitionnum` is set to 0, which should report an error. (#2220 @dl239)
- There are thread races in aggregators if there are concurrent `puts`. (#2472 @zhanghaohit)
- The `limit` clause dose not work if it is used with the `where` and `group by` clauses. (#2447 @aceforeverd)
- The `TaskManager` process will terminate if ZooKeeper disconnects. (#2494 @tobegit3hub)
- The replica cluster dose not create the database if a database is created in the leader cluster (#2488 @dl239)
- When there is data in base tables, deployment with long windows still can be executed (which should report an error). (#2501 @zhanghaohit)
- Other minor bug fixes (#2415 @aceforeverd, #2417 #2434 #2435 #2473 @dl239, #2466 @vagetablechicken)

### Code Refactoring
#2413 @dl239, #2470 #2467 #2468 @vagetablechicken

## [0.6.1] - 2022-08-30

### Features
- Support new build-in functions `last_day` and `regexp_like` (#2262 @HeZean, #2187 @jiang1997)
- Support Jupyter Notebook for the TalkingData use case (#2354 @vagetablechicken)
- Add a new API to disable Saprk logs of the batch engine (#2359 @tobegit3hub)
- Add the use case of precision marketing based on OneFlow (#2267 @Elliezza @vagetablechicken @siqi)
- Support the RPC request timeout in CLI and Python SDK (#2371 @vagetablechicken)
- Improve the documents (#2021 @liuceyim, #2348 #2316 #2324 #2361 #2315 #2323 #2355 #2328 #2360 #2378 #2319 #2350 #2395 #2398 @michelle-qinqin, #2373 @njzyfr, #2370 @tobegit3hub, #2367 #2382 #2375 #2401 @vagetablechicken, #2387 #2394 @dl239, #2379 @aceforeverd, #2403 @lumianph, #2400 gitpod-for-oss @aceforeverd, )
- Other minor features (#2363 @aceforeverd, #2185 @qsliu2017)

### Bug Fixes
- `APIServer` will core dump if no `rs` in `QueryResp`. (#2346 @vagetablechicken)
- Data has not been deleted from `pre-aggr` tables if there are delete operations in a main table. (#2300 @zhanghaohit)
- Task jobs will core dump when enabling `UnsafeRowOpt` with multiple threads in the Yarn cluster. (#2352 #2364 @tobegit3hub)
- Other minor bug fixes (#2336 @dl239, #2337 @dl239, #2385 #2372 @aceforeverd, #2383 #2384 @vagetablechicken)

### Code Refactoring
#2310 @hv789, #2306 #2305 @yeya24, #2311 @Mattt47, #2368 @TBCCC, #2391 @PrajwalBorkar, #2392 @zahyaah, #2405 @wang-jiahua

## [0.6.0] - 2022-08-10

### Highlights

- Add a new toolkit of managing OpenMLDB, currently including a diagnostic tool and a log collector (#2299 #2326 @dl239 @vagetablechicken)
- Support aggregate functions with suffix `_where` using pre-aggregation (#1821 #1841 #2321 #2255 #2321 @aceforeverd @nautaa @zhanghaohit)
- Support a new SQL syntax of `EXCLUDE CURRENT_ROW` (#2053 #2165 #2278 @aceforeverd)
- Add new OpenMLDB ecosystem plugins for DolphinScheduler (#1921 #1955 @vagetablechicken) and Airflow (#2215 @vagetablechicken)

### Other Features

- Support SQL syntax of `DELETE` in SQL and Kafka Connector (#2183 #2257 @dl239)
- Support customized order in the `insert` statement (#2075 @vagetablechicken)
- Add a new use case of TalkingData AdTracking Fraud Detection (#2008 @vagetablechicken)
- Improve the startup script to remove `mon` (#2050 @dl239)
- Improve the performance of offline batch SQL engine (#1882 #1943 #1973 #2142 #2273 #1773 @tobegit3hub)
- Support returning version numbers from TaskManager (#2102 @tobegit3hub)
- Improve the CICD workflow and release procedure (#1873 #2025 #2028 @mangoGoForward)
- Support GitHub Codespaces (#1922 @nautaa)
- Support new built-in functions `char(int)`, `char_length`, `character_length`, `radians`, `hex`, `median` (#1896 #1895 #1897 #2159 #2030 @wuxiaobai24 @HGZ-20 @Ivyee17)
- Support returning result set for a new query API (#2189 @qsliu2017)
- Improve the documents (#1796 #1817 #1818 #2254 #1948 #2227 #2254  #1824 #1829 #1832 #1840 #1842 #1844 #1845 #1848 #1849 #1851 #1858 #1875  #1923 #1925 #1939 #1942 #1945 #1957 #2031 #2054 #2140 #2195 #2304 #2264 #2260 #2257 #2254 #2247 #2240 #2227 #2115 #2126 #2116 #2154 #2152 #2178 #2147 #2146 #2184 #2138 #2145 #2160 #2197 #2198 #2133 #2224 #2223 #2222 #2209 #2248 #2244 #2242 #2241 #2226 #2225 #2221 #2219 #2201 #2291 # 2231 #2196 #2297 #2206 #2238 #2270 #2296 #2317 #2065 #2048 #2088 #2331 #1831 #1945 #2118 @ZtXavier @pearfl @PrajwalBorkar @tobegit3hub @ZtXavier @zhouxh19 @dl239 @vagetablechicken @tobegit3hub @aceforeverd @jmoldyvan @lumianph @bxiiiiii @michelle-qinqin @yclchuxue @redundan3y)

### Bug Fixes

- The SQL engine may produce incorrect results under certain circumstances. (#1950 #1997 #2024 @aceforeverd)
- The `genDDL` function generates incorrect DDL if the SQL is partitioned by multiple columns. (#1956 @dl239)
- The snapshot recovery may fail for disk tables. (#2174 @zhanghaohit)
- `enable_trace` does not work for some SQL queries. (#2292 @aceforeverd)
- Tablets cannot save `ttl` when updating the `ttl` of index. (#1935 @dl239)
- MakeResultSet uses a wrong schema in projection. (#2049 @dl239)
- A table does not exist when deploying SQL by the APIServer (#2205 @vagetablechicken)
- The cleanup for ZooKeep does not work properly. (#2191 @mangoGoForward)

Other minor bug fixes (#2052 #1959 #2253 #2273 #2288 #1964 #2175 #1938 #1963 #1956 #2171 #2036 #2170 #2236 #1867 #1869 #1900 #2162 #2161 #2173 #2190 #2084 #2085 #2034 #1972 #1408 #1863 #1862 #1919 #2093 #2167 #2073 #1803 #1998 #2000 #2012 #2055 #2174 #2036 @Xeonacid @CuriousCorrelation @Shigm1026 @jiang1997 @Harshvardhantomar @nautaa @Ivyee17 @frazie @PrajwalBorkar @dl239 @aceforeverd @tobegit3hub @dl239 @vagetablechicken @zhanghaohit @mangoGoForward @SaumyaBhushan @BrokenArrow1404 @harshlancer)

### Code Refactoring

#1884 #1917 #1953 #1965 #2017 #2033 #2044 @mangoGoForward; #2131 #2130 #2112 #2113 #2104 #2107 #2094 #2068 #2071 #2070 #1982 #1878 @PrajwalBorkar; #2158  #2051 #2037 #2015 #1886 #1857 @frazie; #2100 #2096 @KikiDotPy; #2089 @ayushclashroyale; #1994 @fpetrakov; #2079 kayverly; #2062 @WUBBBB; #1843 @1korenn; #2092 @HeZean; #1984 @0sirusD3m0n; #1976 @Jaguar16; #2086 @marc-marcos; #1999 @Albert-Debbarma;

## [0.5.3] - 2022-07-22

### Bug Fixes
- The SQL file cannot be successfully loaded in the Yarn-Client mode. (#2151 @tobegit3hub)
- The SQL file cannot be successfully loaded in the Yarn-Cluster mode. (#1993 @tobegit3hub)

## [0.5.2] - 2022-06-10

### Features
- Add new built-in functions, including `char_length`, `char`, `radians`, and `replace` (#1895 #1896 #1897 @Ivyee17, #1924 @aceforeverd)
- Add the demo of DolphinScheduler task (#1921 @vagetablechicken)
- Support inserting values with a specified database name (#1929 @dl239)
- Improve window computation with `UnsafeRowOpt` by removing the zipped dataframe (#1882 @tobegit3hub)
- Improve the documents (#1831 @yclchuxue, #1925 @lumianph, #1902 #1923 @vagetablechicken)
- Support GitHub Codespaces (#1922 @nautaa)

### Bug Fixes
- `DistributeWindowIterator::GetKey()` may result in core dump (#1892 aceforeverd)
- `Tablet` does not make `ttl` persistent when updating the ttl of index (#1935 @dl239)
- `TaskManager` startup fails if `LANG=zh_CN.UTF-8` is set (#1912 @vagetablechicken)
- There are duplicate records in `PRE_AGG_META_INFO` (#1919 @nautaa)
- The OpenMLDB Spark fails to fallback to SparkSQL for unsupported functions (#1908 @tobegit3hub)
- Fixing other minor bugs (#1914 aceforeverd, #1900 @mangoGoForward, #1934 @vagetablechicken)

### Code Refactoring
#1899 @auula, #1913 @dl239, #1917 @mangoGoForward, #1803 @SaumyaBhushan, #1870 @Ivyee17, #1886 @frazie

## [0.5.1] - 2022-05-26

### Features
- Support the new OpenMLDB Kafka connector (#1771 @vagetablechicken)
- Support very long SQLs in TaskManager (#1833 @tobegit3hub)
- Support `window union` correctly in the cluster mode (#1855 #1856 @aceforeverd @dl239)
- Support `count_where(*, condition)` in the storage engine (#1841 @nautaa)
- Add a new micro-benchmark tool for performance evaluation (#1800 @dl239)

### Bug Fixes
- Auto creating table throws error when a new ttl is greater than the current ttl. (#1737 @keyu813)
- Offline tasks crash when enabling `UnsafeRowOpt` for continuous windows. (#1773 @tobegit3hub)
- The aggregator is not reset if the table is empty. (#1784 @zhanghaohit)
- The order for window union rows and original rows with the same order key is undefined. (#1802 @aceforeverd)
- Queries with pre-aggregate enabled may crash under certain tests. (#1838 zhanghaohit)
- Ending space in CLI may cause program crash. (#1820 @aceforeverd)
- When creating an engine with empty databases, it cannot execute the command of `USE` database in the Python SDK. (#1854 @vagetablechicken)
- When using the soft copy for csv files, it cannot read offline path with options. (#1872 @vagetablechicken)

### Code Refactoring
#1766 @hiyoyolumi; #1777 @jmoldyvan; #1779 @SohamRatnaparkhi; #1768 @SaumyaBhushan; #1795 @vighnesh-kadam; #1806 @Mount-Blanc; #1978 @wangxinyu666666; #1781 @SaumyaBhushan; #1786 @xuduling; #1810 @IZUMI-Zu; #1824 @bxiiiiii; #1843 @1korenn; #1851 @zhouxh19; #1862 @Ivyee17; #1867, #1869, #1873, #1884 @mangoGoForward; #1863 @Ivyee17; #1815 @jmoldyvan; #1857 @frazie; #1878 @PrajwalBorkar

## [0.5.0] - 2022-05-07

### Highlights

- We have introduced an important performance optimization technique of pre-aggregation, which can significantly improve the performance for a query with time windows containing massive amount of rows, e.g., a few millions. (#1532 #1573 #1583 #1622 #1627 #1672 # 1712 @zhanghaohit @nautaa)
- We have added a new storage engine that supports persistent storage (such as HDD and SSD) for the online SQL engine. Such a storage engine is helpful when a user wants to reduce the cost with acceptable performance degradation. (#1483 @Leowner)
- We have supported C/C++ based User-Defined Functions (UDFs) with dynamic registration to enhance the development experience.  (#1509 #1733 #1700 @dl239 @tobegit3hub)

### Other Features

- Enhance the OpenMLDB Prometheus exporter ( #1584, #1645, #1754 @aceforeverd )
- Support collecting statistics of query response time for online queries ( #1497, #1521 @aceforeverd )
- Support new SQL commands: `SHOW COMPONENTS`, `SHOW TABLE STATUS` (#1380 #1431 #1704 @aceforeverd)
- Support setting global variables (#1310 #1359 #1364 @keyu813 @aceforeverd)
- Support reading Spark configuration files from the CLI (#1600 @tobegit3hub)
- Support using multiple threads for the Spark local mode (#1675 @tobegit3hub)
- Enhance the performance of join by using the Spark's native expression (#1502 tobegit3hub)
- Support the validation for TaskManager configuration (#1262 @tobegit3hub)
- Support tracking unfinished jobs in the TaskManager (#1474 @tobegit3hub)
- Other minor features (#1601 @dl239; #1574 @vagetablechicken; #1546 @keyu813; #1729 @vagetablechicken; #1460 @tobegit3hub)

### Bug Fixes
- Incorrect results when the order of conditions specified in `where` is different from that of the index (#1709 @aceforeverd)
- Incorrect results of `lag`/`at`/`lead` under certain circumstances (#1605 #1739 @aceforeverd)
- Memory leakage in `zk_client` (#1660 @wuxiaobai24)
- No catalog update if the role of a tablet is changed (#1655 @dl239)
- Related bugs about `UnsafeRow` for the offline engine (#1298, #1312, #1326, #1362, #1637, #1381, #1731 @tobegit3hub)
- Incorrect results after adding a new index in the standalone mode (#1721 @keyu813)
- Incorrect results of `SHOW JOBS` under certain circumstances (#1453 @tobegit3hub)
- Incorrect results of the date columns with `UnsafeRowOpt`(#1469 @tobegit3hub)
- Other minor bug fixes (#1698 @kfiring; #1651 @kutlayacar; #1621 @KaidoWang; #1150, #1243 @tobegit3hub; )

### Code Refactoring
#1616 @dl239; #1743 @zjx1319


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

[0.6.3]: https://github.com/4paradigm/OpenMLDB/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/4paradigm/OpenMLDB/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/4paradigm/OpenMLDB/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.5.3...v0.6.0
[0.5.3]: https://github.com/4paradigm/OpenMLDB/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/4paradigm/OpenMLDB/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/4paradigm/OpenMLDB/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.4...v0.5.0
[0.4.4]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.3...v0.4.4
[0.4.3]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/4paradigm/OpenMLDB/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/4paradigm/OpenMLDB/compare/0.2.2...v0.2.3
[0.2.2]: https://github.com/4paradigm/OpenMLDB/compare/v0.2.0...0.2.2
[0.2.0]: https://github.com/4paradigm/OpenMLDB/compare/v0.1.5-pre...v0.2.0

# OpenMLDB Benchmark

OpenMLDB Benchmak tool is used for tesing the performance of OpenMLDB's online SQL engine.

**You may also refer to FEBench (https://github.com/decis-bench/febench), which is a more comprehensive benchmark for real-time feature extraction developed by a third-party, comparing the performance between OpenMLDB and Flink.**

## Requirements

- CentOS 7 / macOS >= 10.15
- JDK 1.8

## Run

1. Compile
    ```bash
    cd benchmark
    mvn clean package
    ```
2. Copy the configuration and package
    ```bash
    mkdir -p /work/benchmark/conf /work/benchmark/lib
    cp target/openmldb-benchmark-0.5.0.jar  /work/benchmark/lib
    cp src/main/resources/conf.properties /work/benchmark/conf
    ```
3. Modify the configuration
    ```
    ZK_CLUSTER=127.0.0.1:32200
    ZK_PATH=/udf_test
    ```
4. Run benchmark
    ```
    cd /work/benchmark
    java -cp conf/:lib/* com._4paradigm.openmldb.benchmark.OpenMLDBPerfBenchmark
    ```

The above testing run with the default confguration. You can modify `WINDOW_NUM`, `WINDOW_SIZE` and `JOIN_NUM` in the confguration file if you want to evaluate the performance impact of those parameters.

Moreover, the default number of threads is 10. You need to set the thread number by `Threads` annotation in `OpenMLDBPerfBenchmark.java` or `OpenMLDBLongWindowBenchmark.java` as below and compile again if you want to test under other thread confguration.
```java
@Threads(10)
```
If you want to test `Throughput`, set `BenchmarkMode` and `OutputTimeUnit` annotation in `OpenMLDBPerfBenchmark.java` or `OpenMLDBLongWindowBenchmark.java` file as below:
```java
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
```

### Long Window Benchmark (The Pre-Aggregate Optimization)
Update `WINDOW_SIZE` in confguration file and execute the following command. 
```
java -cp conf/:lib/ com._4paradigm.openmldb.benchmark.OpenMLDBLongWindowBenchmark
```

Note:
If you want to test specific SQL, you can modify [here](https://github.com/4paradigm/OpenMLDB/blob/main/benchmark/src/main/java/com/_4paradigm/openmldb/benchmark/Util.java#L71)


### Memory Usage Benchmark (Comparison with Redis)
1. Configure [memory.properties](src%2Fmain%2Fresources%2Fmemory.properties)
2. Start the test by executing the following command:
```
java -cp conf/:lib/* com._4paradigm.openmldb.memoryusagecompare.BenchmarkMemoryUsage
```
3. The test report will be printed at the end of the test execution.
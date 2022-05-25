# OpenMLDB Benchmark

OpenMLDB Benchmak tool is used for tesing the performance of OpenMLDB online feature extraction.

## Requirements

- CentOS 7 / macOS >= 10.15
- JDK 1.8

## Run

 1. Compile
    ```bash
    cd java/openmldb-jmh
    ./mvnw clean package
    ```
2. Uncompress the package to `lib` dir and copy the configuration to `conf` dir
    ```bash
    mkdir -p /work/benchmark/conf /work/benchmark/lib
    cp target/openmldb-jmh-0.5.0.jar  /work/benchmark/lib
    cp src/main/resources/conf.properties /work/benchmark/conf
    cd /work/benchmark/lib && jar -xvf openmldb-jmh-0.5.0.jar
    ```
3. Modify the configuration
    ```
    ZK_CLUSTER=127.0.0.1:32200
    ZK_PATH=/udf_test
    ```
4. Run benchmark
    ```
    cd /work/benchmark
    java -cp conf/:lib/ com._4paradigm.sql.jmh.openmldb.OpenMLDPerfBenchmark
    ```

The above testing run with the default confguration. It need to modify `WINDOW_NUM`, `WINDOW_SIZE` and `JOIN_NUM` in confguration file if you want to test other scenes. More over, the default benmark threads is 10. It need to set thead number by `Threads` annotation in source file and compile again if you want to test under other thread confguration.

### Long Window Benchmark
Update `WINDOW_SIZE` in confguration file and execute the following command. 
```
java -cp conf/:lib/ com._4paradigm.sql.jmh.openmldb.OpenMLDLongWindowBenchmark
```

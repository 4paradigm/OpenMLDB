# OpenMLDB JMH

Benchmark related code for [OpenMLDB](https://github.com/4paradigm/OpenMLDB)

## Status

WIP

## Requirements

- Linux
- JDK 1.8

## Run

```bash
# compile
./mvnw clean package

# run a benchmark class
java -cp target/openmldb-jmh-0.2.3-SNAPSHOT.jar com._4paradigm.sql.jmh.openmldb.OpenMLDBParameterizedQueryBenchmark
```


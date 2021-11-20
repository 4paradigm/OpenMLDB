# Build & Install

## Build on Linux
1. Download source code
    ```bash
    git clone git@github.com:4paradigm/OpenMLDB.git
    cd OpenMLDB
    ```
2. Download docker image, which is used to provide necessary tools and dependencies for building
    ```bash
    docker pull ghcr.io/4paradigm/hybridsql:0.4.0
    ```
3. Start docker and map local dir in docker
    ```bash
    docker run -v `pwd`:/OpenMLDB -it ghcr.io/4paradigm/hybridsql:0.4.0 bash
    ```
4. Compile OpenMLDB
    ```
    make
    ```

### Extra Options for make

customize make behavior by passing following arguments. E.g, change the build type to Debug:

```bash
make CMAKE_BUILD_TYPE=Debug
```

- CMAKE_BUILD_TYPE

  Default: RelWithDebInfo

- SQL_PYSDK_ENABLE

  enable build python sdk

  Default: OFF

- SQL_JAVASDK_ENABLE

  enable build java sdk

  Default: OFF

- TESTING_ENABLE

  enable build test targets

  Default: ON


## Optimized Spark Distribution for OpenMLDB (Optional)

[OpenMLDB Spark Distribution](https://github.com/4paradigm/spark) is the fork of [Apache Spark](https://github.com/apache/spark) which has more optimization for machine learning scenarios. It provides native LastJoin implementation and achieves 10x~100x performance improvement. You can use OpenMLDB Spark Distribution just like the standard Spark with the same Java/Scala/Python/SQL APIs.

Download the pre-built OpenMLDB Spark distribution.

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb0.2.3/spark-3.0.0-bin-openmldbspark.tgz
```

Or download the source code and compile from scratch.

```bash
git clone https://github.com/4paradigm/spark.git

cd ./spark/

./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone
```

Then we can run PySpark or SparkSQL just like standard Spark distribution.

```bash
tar xzvf ./spark-3.0.0-bin-openmldbspark.tgz

cd spark-3.0.0-bin-openmldbspark/

export SPARK_HOME=`pwd`
```

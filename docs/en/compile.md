# Build & Install

## Build on Linux
1. Download source code
    ```bash
    git clone git@github.com:4paradigm/OpenMLDB.git
    cd OpenMLDB
    ```
2. Download the docker image for building, which is used to provide necessary tools and dependencies for building
    ```bash
    docker pull 4pdosc/hybridsql:0.4.0
    ```
3. Start the docker with mapping the local OpenMLDB source code directory into the container
    ```bash
    docker run -v `pwd`:/OpenMLDB -it 4pdosc/hybridsql:0.4.0 bash
    ```
4. Compile OpenMLDB in the docker
    ```
    cd /OpenMLDB
    make
    ```

### Extra Options for `make`

You can customize the `make` behavior by passing following arguments, e.g., changing the build mode to `Debug` instead of `Release`:

```bash
make CMAKE_BUILD_TYPE=Debug
```

- CMAKE_BUILD_TYPE

  Default: RelWithDebInfo

- SQL_PYSDK_ENABLE: enabling building the Python SDK

  Default: OFF

- SQL_JAVASDK_ENABLE: enabling building the Java SDK

  Default: OFF

- TESTING_ENABLE: enabling building the test targets

  Default: ON


## Optimized Spark Distribution for OpenMLDB (Optional)

[OpenMLDB Spark Distribution](https://github.com/4paradigm/spark) is the fork of [Apache Spark](https://github.com/apache/spark). It adopts specific optimization techniques for OpenMLDB. It provides native `LastJoin` implementation and achieves 10x~100x performance improvement compared with the original Spark distribution. The Java/Scala/Python/SQL APIs of the OpenMLDB Spark distribution are fully compatible with the standard Spark distribution.

1. Downloading the pre-built OpenMLDB Spark distribution:

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb0.2.3/spark-3.0.0-bin-openmldbspark.tgz
```

Alternatively, you can also download the source code and compile from scratch:

```bash
git clone https://github.com/4paradigm/spark.git
cd ./spark/
./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone
```

2. Setting up the default Spark home as the OpenMLDB Spark distribution

```bash
tar xzvf ./spark-3.0.0-bin-openmldbspark.tgz
cd spark-3.0.0-bin-openmldbspark/
export SPARK_HOME=`pwd`
```

3. Now you are all set to run OpenMLDB by enjoying the performance speedup from this optimized Spark distribution.

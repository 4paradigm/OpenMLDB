Build & Install
===============

# Quick start

[quick-start]: quick-start

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
    ```bash
    cd /OpenMLDB
    make
    ```
5. Install OpenMLDB, will installed into `${PROJECT_ROOT}/openmldb` by default
    ```bash
    make install
    ```

# Build

[build]: build

## Hardware Requirements

- **Memory**: 4GB RAM minimum, 8GB+ recommended.
- **Disk Space**: >=25GB of free disk space for full compilation.
- **Operating System**: A 64-bit installation of Linux or macOS >= 10.14 

## Prerequisites

Make sure those tools are installed

- gcc 8 or later
- cmake 3.20 or later
- jdk 8
- python3, python setuptools, python wheel
- apache maven 3.x
- if you'd like compile thirdparty from source, checkout [third-party's requirement](../../third-party/README.md) for extra dependencies

## Build OpenMLDB

  Build commands are same as [Quick start](#quick-start)

  ```bash
  $ cd OpenMLDB
  $ make
  $ make install
  ```

## Extra Options for `make`

You can customize the `make` behavior by passing following arguments, e.g., changing the build mode to `Debug` instead of `Release`:

```bash
make CMAKE_BUILD_TYPE=Debug
```

- OPENMLDB_BUILD_DIR: Binary build directory

  Default: ${PROJECT_ROOT}/build

- CMAKE_BUILD_TYPE

  Default: RelWithDebInfo

- CMAKE_INSTALL_PREFIX

  Default: ${PROJECT_ROOT}/openmldb

- SQL_PYSDK_ENABLE: enabling building the Python SDK

  Default: OFF

- SQL_JAVASDK_ENABLE: enabling building the Java SDK

  Default: OFF

- TESTING_ENABLE: enabling building the test targets

  Default: OFF

- NPROC: parallel build number

  Default: $(nproc)

- CMAKE_EXTRA_FLAGS: extra flags passed to cmake

  Default: ‘’

- BUILD_BUNDLED: compile thirdparty from source instead download pre-compiled

  Default: OFF


## Troubleshooting

[build-troubleshooting]: build-troubleshooting

- If the host machine's resource is limited, e.g a VM when 4G memory, it is advised to turn off parallel build by change the `NPROC` variable:
    ```bash
    make NPROC=1
    ```
- By default, pre-compiled thirdparty is downloaded from [hybridsql-assert](https://github.com/4paradigm/hybridsql-asserts/releases), which support CentOS 7, Ubuntu 20.04 and macoS. If your host is not in the list or come with unexpected link issues, it is advised to compile thirdparty from source as well. Note  thirdparty compilation may take extra time to finish, approximately 1 hour for 2 core & 7GB machine
  ```bash
  make BUILD_BUNDLED=ON
  ```

# Optimized Spark Distribution for OpenMLDB (Optional)

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

2. Setting up the environment variable `SPARK_HOME` to make the OpenMLDB Spark distribution for OpenMLDB or other Spark applications

```bash
tar xzvf ./spark-3.0.0-bin-openmldbspark.tgz
cd spark-3.0.0-bin-openmldbspark/
export SPARK_HOME=`pwd`
```

3. Now you are all set to run OpenMLDB by enjoying the performance speedup from this optimized Spark distribution.

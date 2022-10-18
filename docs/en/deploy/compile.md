# Build

## 1. Quick Start

[quick-start]: quick-start

This section describes the steps to compile and use OpenMLDB inside its official docker image [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql).
The docker image has packed required tools and dependencies, so there is no need to set them up separately. To compile without the official docker image, refer to the section [Detailed Instructions for Build](#detailed-instructions-for-build) below.

Keep in mind that you should always use the same version of both compile image and [OpenMLDB version](https://github.com/4paradigm/OpenMLDB/releases). This section demonstrates compiling for [OpenMLDB v0.6.3](https://github.com/4paradigm/OpenMLDB/releases/tag/v0.6.3) under `hybridsql:0.6.3` ，If you prefer to compile on the latest code in `main` branch, pull `hybridsql:latest` image instead.

1. Pull the docker image

   ```bash
    docker pull 4pdosc/hybridsql:0.6
   ```

2. Create a docker container with the hybridsql docker image

   ```bash
   docker run -it 4pdosc/hybridsql:0.6 bash
   ```

3. Download the OpenMLDB source code inside the docker container, and setting the branch into v0.6.3

   ```bash
   cd ~
   git clone -b v0.6.3 https://github.com/4paradigm/OpenMLDB.git
   ```

4. Compile OpenMLDB

   ```bash
   cd ~/OpenMLDB
   make
   ```

5. Install OpenMLDB that will be installed into `${PROJECT_ROOT}/openmldb` by default

   ```bash
   make install
   ```

Now you've finished the compilation job, and you may try run OpenMLDB inside the docker container.

## 2. Detailed Instructions for Build

[build]: build

### 2.1. Hardware Requirements

- **Memory**: 8GB+ recommended.
- **Disk Space**: >=25GB of free disk space for full compilation.
- **Operating System**: CentOS 7, Ubuntu 20.04 or macOS >= 10.15, other systems are not carefully tested but issue/PR welcome

Note: By default, the parallel build is disabled, and it usually takes an hour to finish all the compile jobs. You can enable the parallel build by tweaking the `NPROC` option if your machine's resource is enough. This will reduce the compile time but also consume more memory. For example, the following command set the number of concurrent build jobs to 4:

```bash
make NPROC=4
```

### 2.2. Prerequisites

Make sure those tools are installed

- gcc >= 8 or AppleClang >= 12.0.0
- cmake 3.20 or later
- jdk 8
- python3, python setuptools, python wheel
- If you'd like to compile thirdparty from source, checkout the [third-party's requirement](../../third-party/README.md) for extra dependencies

### 2.3. Build and Install OpenMLDB

Building OpenMLDB requires certain thirdparty dependencies. Hence a Makefile is provided as a convenience to setup thirdparty dependencies automatically and run CMake project in a single command `make`. The `make` command offers three methods to compile, each manages thirdparty differently:

- **Method One: Build and Run Inside Docker:** Using [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql) docker image, the thirdparty is already bundled inside the image and no extra steps are required, refer to above section [Quick Start](#quick-start)
- **Method Two: Download Pre-Compiled Thirdparty:**  Command is `make && make install`. It downloads necessary prebuild libraries from [hybridsql-assert](https://github.com/4paradigm/hybridsql-asserts/releases) and [zetasql](https://github.com/4paradigm/zetasql/releases).  Currently it supports CentOS 7, Ubuntu 20.04 and macOS.
- **Method Three: Compile Thirdparty from Source:** This is the suggested way if the host system is not in the supported list for pre-compiled thirdparty (CentOS 7, Ubuntu 20.04 and macOS). Note that when compiling thirdparty for the first time requires extra time to finish, approximately 1 hour on a 2 core & 7 GB machine. To compile thirdparty from source, please pass `BUILD_BUNDLED=ON` to `make`:
  
   ```bash
   make BUILD_BUNDLED=ON
   make install
   ```

All of the three methods above will install OpenMLDB binaries into `${PROJECT_ROOT}/openmldb` by default, you may tweak the installation directory with the option `CMAKE_INSTALL_PREFIX` (refer the following section [Extra options for `make`](#24-extra-options-for-make)).

### 2.4. Extra Options for `make`

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

- NPROC: the number of parallel build jobs

  Default: 1

- CMAKE_EXTRA_FLAGS: extra flags passed to cmake

  Default: ‘’

- BUILD_BUNDLED: compile thirdparty from source instead download pre-compiled

  Default: OFF

- TCMALLOC_ENABLE: expose application memory info by tcmalloc

  Default: ON

- OPENMLDB_BUILD_TARGET: If you only want to build some targets, not all, e.g. only build a test `ddl_parser_test`, you can set it to `ddl_parser_test`. Multiple targets may be given, separated by spaces. It can reduce the build time, reduce the build output, save the storage space.

  Default: all

## 3. Optimized Spark Distribution for OpenMLDB

[OpenMLDB Spark Distribution](https://github.com/4paradigm/spark) is the fork of [Apache Spark](https://github.com/apache/spark). It adopts specific optimization techniques for OpenMLDB. It provides native `LastJoin` implementation and achieves 10x~100x performance improvement compared with the original Spark distribution. The Java/Scala/Python/SQL APIs of the OpenMLDB Spark distribution are fully compatible with the standard Spark distribution.

1. Downloading the pre-built OpenMLDB Spark distribution:

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.6.3/spark-3.2.1-bin-openmldbspark.tgz
```

Alternatively, you can also download the source code and compile from scratch:

```bash
git clone https://github.com/4paradigm/spark.git
cd ./spark/
./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone
```

2. Setting up the environment variable `SPARK_HOME` to make the OpenMLDB Spark distribution for OpenMLDB or other Spark applications

```bash
tar xzvf ./spark-3.2.1-bin-openmldbspark.tgz
cd spark-3.2.1-bin-openmldbspark/
export SPARK_HOME=`pwd`
```

3. Now you are all set to run OpenMLDB by enjoying the performance speedup from this optimized Spark distribution.

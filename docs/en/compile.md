Build OpenMLDB
================

# Quick Start

[quick-start]: quick-start

This section describes the steps to compile and use OpenMLDB inside its official docker image [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql).
The docker image has packed required tools and dependencies, so there is no need to setup them separately. To compile without the official docker image, refer to the section [Detailed Instructions for Build](#detailed-instructions-for-build) bellow.

1. Pull the docker image

   ```bash
    docker pull 4pdosc/hybridsql:0.4.1
   ```

2. Create a docker container with the hybridsql docker image

   ```bash
   docker run -it 4pdosc/hybridsql:0.4.1 bash
   ```

3. Download the OpenMLDB source code inside docker container

   ```bash
   cd ~
   git clone https://github.com/4paradigm/OpenMLDB.git
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

# Detailed Instructions for Build

[build]: build

## Hardware Requirements

- **Memory**: 8GB+ recommended.
- **Disk Space**: >=25GB of free disk space for full compilation.
- **Operating System**: CentOS 7, Ubuntu 20.04 or macOS >= 10.15, other systems are not carefully tested but issue/PR welcome

💡 Note: by default parallel build is disabled, and it usually takes an hour to finish all the compile jobs. You can enable the parallel build by tweaking the `NPROC` option
if your machine's resource is enough. This will reduce the compile time but also consume more memory, e.g., following command set the number of concurrent build jobs to 4:
```bash
make NPROC=4
```

## Prerequisites

Make sure those tools are installed

- gcc >= 8 or AppleClang >= 12.0.0
- cmake 3.20 or later
- jdk 8
- python3, python setuptools, python wheel
- apache maven 3.3.9 or later
- if you'd like compile thirdparty from source, checkout [third-party's requirement](../../third-party/README.md) for extra dependencies

## Build and Install OpenMLDB

Building OpenMLDB requires certain thirdparty dependencies. Hence a Makefile is provided as a convenience to setup thirdparty dependencies automatically and run CMake project in a single command `make`. The `make` command offers three methods to compile, each manages thirdparty differently:

- **Method one: build and run inside docker:** Using [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql) docker image, thirdparty already bundled inside image and no extra steps may take, refer to above section [Quick Start](#quick-start)
- **Method two: download pre-compiled thirdparty:** It downloads necessary prebuild libraries from [hybridsql-assert](https://github.com/4paradigm/hybridsql-asserts/releases). This is the default behavior when building outside a hybridsql container. Currently it supports CentOS 7, Ubuntu 20.04 and macOS. The default command to build and install is `make && make install`
- **Method three: compile thirdparty from source:** This is the suggested way if the host system is not in the supported list for pre-compiled thirdparty (CentOS 7, Ubuntu 20.04 and macOS). Note compiling thirdparty at the first time takes extra time to finish, approximately 1 hour on a 2 core & 7 GB machine. To compile thirdparty from source, please pass `BUILD_BUNDLED=ON` to `make`:
   ```bash
   make BUILD_BUNDLED=ON
   make install
   ```

All of the three methods above will install OpenMLDB binaries into `${PROJECT_ROOT}/openmldb` by default, you may tweak the installation directory with the option `CMAKE_INSTALL_PREFIX` (refer the following section [Extra options for `make`](#make-opts)).

## Extra Options for `make`

[make-opts]: make-opts

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

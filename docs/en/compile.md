Build OpenMLDB
================

# Quick start

[quick-start]: quick-start

This section describe the necessary steps to compile OpenMLDB inside the official docker image [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql).
The docker image bundled required tools and dependencies so there is no need to setup them separately. To compile without the official docker image, refer the [Detailed instructions for build](#build) section bellow.

1. Pull the docker image

   ```bash
    docker pull 4pdosc/hybridsql:0.4.0
   ```

2. Create a docker container with hybridsql docker image

   ```bash
   docker run -it 4pdosc/hybridsql:0.4.0 bash
   ```

3. Download source code inside docker container

   ```bash
   cd ~
   git clone https://github.com/4paradigm/OpenMLDB.git
   ```

4. Compile OpenMLDB

   ```bash
   cd ~/OpenMLDB
   make
   ```

5. Install OpenMLDB, will installed into `${PROJECT_ROOT}/openmldb` by default

   ```bash
   make install
   ```

Now you've accomplished the compilation job, and you may try run OpenMLDB inside the docker container.

# Detailed instructions for build

[build]: build

## Hardware Requirements

- **Memory**: 8GB+ recommended.
- **Disk Space**: >=25GB of free disk space for full compilation.
- **Operating System**: CentOS 7, Ubuntu 20.04 or macOS >= 10.14, other system is not well tested but issue/PR welcome

💡 Note: by default parallel build is not enabled, and it usually takes an hour to finish the compile job. You can enable parallel build by tweaking the `NPROC` option
if you think your machine's resource is enough. This will decrease compile time but also require more memory. E.g. following command set parallel build number to 4, which will run parallelly with four cores:
```bash
make NPROC=4
```

## Prerequisites

Make sure those tools are installed

- gcc 8 or later
- cmake 3.20 or later
- jdk 8
- python3, python setuptools, python wheel
- apache maven 3.3.9 or later
- if you'd like compile thirdparty from source, checkout [third-party's requirement](../../third-party/README.md) for extra dependencies

## Build and install OpenMLDB

OpenMLDB require some thirdparty dependencies installed first in order to build successfully. Hence a Makefile is provided as a convenience to setup thirdparty dependencies automatically and run CMake project in a single command `make`.
`make` offers three methods to compile, each manage thirdparty differently:

- **Method one: build and run inside docker:** using [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql) docker image, thirdparty already bundled inside image and no extra steps may take, refer above section [Quick Start](#quick-start)
- **Method two: download pre-compiled thirdparty:** it download necessary archive from [hybridsql-assert](https://github.com/4paradigm/hybridsql-asserts/releases). This is the default behavior when build outside a hybridsql container, currently support CentOS 7, Ubuntu 20.04 and macOS. The commands to build and install is `make && make install`
- **Method three: compile thirdparty from source:** this is the advised way if the host system is not in the supported list for pre-compiled thirdparty (CentOS 7, Ubuntu 20.04 and macOS). Note compile thirdparty at the first time may take extra time to finish, approximately 1 hour on a 2 core & 7GB machine. To compile thirdparty from source, pass `BUILD_BUNDLED=ON` to `make`:
   ```bash
   make BUILD_BUNDLED=ON
   make install
   ```

All of the three methods above will install into `${PROJECT_ROOT}/openmldb` by default, you may tweak the installation directory with the option `CMAKE_INSTALL_PREFIX` (refer the following section [Extra options for `make`](#make-opts)).

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

- NPROC: parallel build number

  Default: 1

- CMAKE_EXTRA_FLAGS: extra flags passed to cmake

  Default: ‘’

- BUILD_BUNDLED: compile thirdparty from source instead download pre-compiled

  Default: OFF


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

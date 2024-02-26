# Compilation from Source Code

## Compile and Use in Docker Container

This section describes the steps to compile and use OpenMLDB inside its official docker image [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql), mainly for quick start and development purposes in the docker container.
The docker image has packed the required tools and dependencies, so there is no need to set them up separately. To compile without the official docker image, refer to the section [Detailed Instructions for Build](#detailed-instructions-for-build) below.

Keep in mind that you should always use the same version of both compile image and [OpenMLDB version](https://github.com/4paradigm/OpenMLDB/releases). This section demonstrates compiling for [OpenMLDB v0.8.5](https://github.com/4paradigm/OpenMLDB/releases/tag/v0.8.5) under `hybridsql:0.8.5` ，If you prefer to compile on the latest code in `main` branch, pull `hybridsql:latest` image instead.

1. Pull the docker image

   ```bash
    docker pull 4pdosc/hybridsql:0.8
   ```

2. Create a docker container

   ```bash
   docker run -it 4pdosc/hybridsql:0.8 bash
   ```

3. Download the OpenMLDB source code inside the docker container, and set the branch into v0.8.5

   ```bash
   cd ~
   git clone -b v0.8.5 https://github.com/4paradigm/OpenMLDB.git
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

Now you've finished the compilation job, you may try running OpenMLDB inside the docker container.

## Detailed Instructions for Build

This chapter discusses compiling source code without relying on pre-built container environments.

### Hardware Requirements

- **Memory**: 8GB+ recommended.
- **Disk Space**: >=25GB of free disk space for full compilation.
- **Operating System**: CentOS 7, Ubuntu 20.04 or macOS >= 10.15, other systems are not carefully tested but issue/PR welcome
- **CPU Architecture**: Currently, only x86 architecture is supported, and other architectures like ARM are not supported at the moment (please note that running x86 images on heterogeneous systems like M1 Mac is also not supported at this time).

💡 Note: By default, the parallel build is disabled, and it usually takes an hour to finish all the compile jobs. You can enable the parallel build by tweaking the `NPROC` option if your machine's resource is enough. This will reduce the compile time but also consume more memory. For example, the following command sets the number of concurrent build jobs to 4:

```bash
make NPROC=4
```

### Dependencies
- gcc >= 8 or AppleClang >= 12.0.0
- cmake 3.20 or later ( recommended < cmake 3.24)
- jdk 8
- python3, python setuptools, python wheel
- If you'd like to compile thirdparty from source, checkout the [third-party's requirement](../../third-party/README.md) for extra dependencies

### Build and Install OpenMLDB

Building OpenMLDB requires certain thirdparty dependencies. Hence a `Makefile` is provided as a convenience to setup thirdparty dependencies automatically and run CMake project in a single command `make`. The `make` command offers three methods to compile, each manages thirdparty differently:

- **Method One: Download Pre-Compiled Thirdparty:**  Command is `make && make install`. It downloads necessary prebuild libraries from [hybridsql-assert](https://github.com/4paradigm/hybridsql-asserts/releases) and [zetasql](https://github.com/4paradigm/zetasql/releases).  Currently it supports CentOS 7, Ubuntu 20.04 and macOS.
- **Method Two: Compile Thirdparty from Source:** This is the suggested way if the host system is not in the supported list for pre-compiled thirdparty (CentOS 7, Ubuntu 20.04 and macOS). Note that when compiling thirdparty for the first time requires extra time to finish, approximately 1 hour on a 2 core & 8 GB machine. To compile thirdparty from source, please pass `BUILD_BUNDLED=ON` to `make`:
  
   ```bash
   make BUILD_BUNDLED=ON
   make install
   ```

All of the three methods above will install OpenMLDB binaries into `${PROJECT_ROOT}/openmldb` by default, you may tweak the installation directory with the option `CMAKE_INSTALL_PREFIX` (refer to the following section [Extra Parameters for `make`](#extra-parameters-for-make) ).

### Extra Parameters for `make`

You can customize the `make` behavior by passing the following arguments, e.g., changing the build mode to `Debug` instead of `Release`:

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

- OPENMLDB_BUILD_TARGET: If you only want to build some targets, not all, e.g. only build a test `ddl_parser_test`, you can set it to `ddl_parser_test`. Multiple targets may be given, separated by spaces. It can reduce build time, reduce build output, and save storage space.

  Default: all

- THIRD_PARTY_CMAKE_FLAGS: You can use this to configure additional parameters when compiling third-party dependencies. For instance, to specify concurrent compilation for each third-party project, you can set` THIRD_PARTY_CMAKE_FLAGS` to `-DMAKEOPTS=-j8`. Please note that NPROC does not affect third-party compilation; multiple third-party projects will be executed sequentially.
  
  Default: ''

### Build Java SDK with Multi Processes

```
make SQL_JAVASDK_ENABLE=ON NPROC=4
```

The built jar packages are in the `target` path of each submodule. If you want to use the jar packages built by yourself, please DO NOT add them by systemPath(may get `ClassNotFoundException` about Protobuf and so on, requires a little work in compile and runtime phase). The better way is, use `mvn install -DskipTests=true -Dscalatest.skip=true -Dwagon.skip=true -Dmaven.test.skip=true -Dgpg.skip` to install them in local m2 repository, your project will use them.

## Optimized Spark Distribution for OpenMLDB

[OpenMLDB Spark Distribution](https://github.com/4paradigm/spark) is the fork of [Apache Spark](https://github.com/apache/spark). It adopts specific optimization techniques for OpenMLDB. It provides native `LastJoin` implementation and achieves 10x~100x performance improvement compared with the original Spark distribution. The Java/Scala/Python/SQL APIs of the OpenMLDB Spark distribution are fully compatible with the standard Spark distribution.

1. Downloading the pre-built OpenMLDB Spark distribution:

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.8.5/spark-3.2.1-bin-openmldbspark.tgz
```

Alternatively, you can also download the source code and compile from scratch:

```bash
git clone https://github.com/4paradigm/spark.git
cd ./spark/
./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone -Phive -Phive-thriftserver
```

2. Setting up the environment variable `SPARK_HOME` to make the OpenMLDB Spark distribution for OpenMLDB or other Spark applications

```bash
tar xzvf ./spark-3.2.1-bin-openmldbspark.tgz
cd spark-3.2.1-bin-openmldbspark/
export SPARK_HOME=`pwd`
```

3. Now you are all set to run OpenMLDB by enjoying the performance speedup from this optimized Spark distribution.


## Build for Other OS
As previously mentioned, if you want to run OpenMLDB or the SDK on a different OS, you will need to compile from the source code. We provide quick compilation solutions for several operating systems. For other OS, you'll need to perform source code compilation on your own.

### Centos 6 or other glibc Linux OS
#### Local Compilation
To compile a version compatible with CentOS 6, you can use Docker and the `steps/centos6_build.sh` script. As shown below, we use the current directory as the mount directory and place the compilation output locally.

```bash
git clone https://github.com/4paradigm/OpenMLDB.git
cd OpenMLDB
docker run -it -v`pwd`:/root/OpenMLDB ghcr.io/4paradigm/centos6_gcc7_hybridsql bash
```
Execute the compilation script within the container, and the output will be in the "build" directory. If there are failures while downloading `bazel` or `icu4c` during compilation, you can use the image sources provided by OpenMLDB by configuring the environment variable `OPENMLDB_SOURCE=true`. Various environment variables that can be used with "make" will also work, as shown below.

```bash
cd OpenMLDB
bash steps/centos6_build.sh
# THIRD_PARTY_CMAKE_FLAGS=-DMAKEOPTS=-j8 bash steps/centos6_build.sh # run fast when build single project
# OPENMLDB_SOURCE=true bash steps/centos6_build.sh
# SQL_JAVASDK_ENABLE=ON SQL_PYSDK_ENABLE=ON NPROC=8 bash steps/centos6_build.sh # NPROC will build openmldb in parallel, thirdparty should use THIRD_PARTY_CMAKE_FLAGS
```

For a local compilation with a 2.20GHz CPU, SSD hard drive, and 32 threads to build both third-party libraries and the OpenMLDB core, the approximate timeframes are as follows:
`THIRD_PARTY_CMAKE_FLAGS=-DMAKEOPTS=-j32 SQL_JAVASDK_ENABLE=ON SQL_PYSDK_ENABLE=ON NPROC=32 bash steps/centos6_build.sh`
- third-party (excluding source code download time): Approximately 40 minutes:
  - Zetasql patch: 13 minutes
  - Compilation of all third-party dependencies: 30 minutes
- OpenMLDB core, including Python and Java native components: Approximately 12 minutes

Please note that these times can vary depending on your specific hardware and system performance. The provided compilation commands and environment variables are optimized for multi-threaded compilation, which can significantly reduce build times.

#### Cloud Compilation

After forking the OpenMLDB repository, you can trigger the `Other OS Build` workflow in `Actions`, and the output will be available in the `Actions` `Artifacts`. Here's how to configure the workflow:

- Do not change the `Use workflow from` setting to a specific tag; it can be another branch.
- Choose the desired `OS name`, which in this case is `centos6`.
- If you are not compiling the main branch, provide the name of the branch, tag (e.g., v0.8.5), or SHA you want to compile in the `The branch, tag, or SHA to checkout, otherwise use the branch` field.
- The compilation output will be accessible in "runs", as shown in an example [here](https://github.com/4paradigm/OpenMLDB/actions/runs/6044951902).
  - The workflow will definitely produce the OpenMLDB binary file.
  - If you don't need the Java or Python SDK, you can configure `java sdk enable` or `python sdk enable` to be "OFF" to save compilation time.

Please note that this compilation process involves building third-party dependencies from source code, and it may take a while to complete due to limited resources. The approximate time for this process is around 3 hours and 5 minutes (2 hours for third-party dependencies and 1 hour for OpenMLDB). However, the workflow caches the compilation output for third-party dependencies, so the second compilation will be much faster, taking approximately 1 hour and 15 minutes for OpenMLDB.

### Macos 10.15, 11

MacOS doesn't require compiling third-party dependencies from source code, so compilation is relatively faster, taking about 1 hour and 15 minutes. Local compilation is similar to the steps outlined in the [Detailed Instructions for Build](#detailed-instructions-for-build) and does not require compiling third-party dependencies (`BUILD_BUNDLED=OFF`). For cloud compilation on macOS, trigger the `Other OS Build` workflow in `Actions` with the specified macOS version (`os name` as `macos10` or `macos11`). You can also disable Java or Python SDK compilation if they are not needed, by setting `java sdk enable` or `python sdk enable` to `OFF`.




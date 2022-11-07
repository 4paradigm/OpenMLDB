# 编译

## 快速开始 - 在 docker 容器内编译和使用

此节介绍在官方编译镜像 [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql) 中编译 OpenMLDB。镜像内置了编译所需要的工具和依赖，因此不需要额外的步骤单独配置它们。关于基于非 docker 的编译使用方式，请参照下面的 [编译详细说明](#编译详细说明) 章节。

关于编译镜像版本，需要注意拉取的镜像版本和 [OpenMLDB 发布版本](https://github.com/4paradigm/OpenMLDB/releases)保持一致。以下例子演示了在 `hybridsql:0.6.5` 镜像版本上编译 [OpenMLDB v0.6.5](https://github.com/4paradigm/OpenMLDB/releases/tag/v0.6.5) 的代码，如果要编译最新 `main` 分支的代码，则需要拉取 `hybridsql:latest` 版本镜像。

1. 下载 docker 镜像
    ```bash
    docker pull 4pdosc/hybridsql:0.6
    ```

2. 启动 docker 容器
    ```bash
    docker run -it 4pdosc/hybridsql:0.6 bash
    ```

3. 在 docker 容器内, 克隆 OpenMLDB, 并切换分支到 v0.6.5
    ```bash
    cd ~
    git clone -b v0.6.5 https://github.com/4paradigm/OpenMLDB.git
    ```

4. 在 docker 容器内编译 OpenMLDB
    ```bash
    cd OpenMLDB
    make
    ```

5. 安装 OpenMLDB, 默认安装到`${PROJECT_ROOT}/openmldb`
    ```bash
    make install
    ```
    至此， 你已经完成了在 docker 容器内的编译工作，你现在可以在容器内开始使用 OpenMLDB 了。

## 编译详细说明

### 硬件要求

- **内存**: 推荐 8GB+.
- **硬盘**: 全量编译需要至少 25GB 的空闲磁盘空间
- **操作系统**: CentOS 7, Ubuntu 20.04 或者 macOS >= 10.15, 其他系统未经测试，欢迎提 issue 或 PR
- **CPU 架构**: 目前仅支持 x86 架构，暂不支持例如 ARM 等架构 (注意在 M1 Mac 上异构运行 x86 镜像同样暂不支持)

💡 注意：默认关闭了并发编译，其典型的编译时间大约在一小时左右。如果你认为编译机器的资源足够，可以通过调整编译参数 `NPROC` 来启用并发编译功能。这会减少编译所需要的时间但也需要更多但内存。例如下面命令将并发编译数设置成使用四个核进行并发编译：
```bash
make NPROC=4
```

### 依赖工具

- gcc >= 8 或者 AppleClang >= 12.0.0
- cmake 3.20 或更新版本
- jdk 8
- python3, python setuptools, python wheel
- 如果需要从源码编译 thirdparty, 查看 [third-party's requirement](../../third-party/README.md) 里的额外要求

### 编译和安装 OpenMLDB

成功编译 OpenMLDB 要求依赖的第三方库预先安装在系统中。因此添加了一个 `Makefile`, 将第三方依赖自动安装和随后执行 CMake 编译浓缩到一行 `make` 命令中。`make` 提供了三种编译方式，对第三方依赖进行不同的管理方式：

- **方式一：docker 容器内编译和使用：** 使用 [hybridsql](https://hub.docker.com/r/4pdosc/hybridsql) docker 镜像进行编译和使用，第三方依赖已经包括在镜像中所以不需要额外的操作，操作步骤参照上面的 [快速开始](#快速开始) 章节。
- **方式二：自动下载预编译库：** 编译安装命令为：`make && make install`。编译脚本自动从 [hybridsql](https://github.com/4paradigm/hybridsql-asserts/releases) 和[zetasql](https://github.com/4paradigm/zetasql/releases)两个仓库下载必须的预编译好的三方库。目前提供 CentOS 7, Ubuntu 20.04 和 macOS 的预编译包。
- **方式三：完整源代码编译：** 如果操作系统不在支持的系统列表中(CentOS 7, Ubuntu 20.04, macOS)，从源码编译是推荐的方式。注意首次编译三方库可能需要更多的时间，在一台2核7G内存机器大约需要一个小时。从源码编译安装第三方库, 传入 `BUILD_BUNDLED=ON`:
   ```bash
   make BUILD_BUNDLED=ON
   make install
   ```

以上 OpenMLDB 安装成功的默认目录放在 `${PROJECT_ROOT}/openmldb`，可以通过修改参数 `CMAKE_INSTALL_PREFIX` 更改安装目录（详见下面章节 [`make` 额外参数](#make-额外参数)）。

### `make` 额外参数

控制 `make` 的行为. 例如，将默认编译模式改成 Debug:

```bash
make CMAKE_BUILD_TYPE=Debug
```

- OPENMLDB_BUILD_DIR: 代码编译路径

  默认: ${PROJECT_ROOT}/build

- CMAKE_BUILD_TYPE

  默认: RelWithDebInfo

- CMAKE_INSTALL_PREFIX: 安装路径

  默认: ${PROJECT_ROOT}/openmldb

- SQL_PYSDK_ENABLE：是否编译 Python SDK

  默认: OFF

- SQL_JAVASDK_ENABLE：是否编译 Java SDK

  默认: OFF

- TESTING_ENABLE：是否编译测试目标

  默认: OFF

- NPROC: 并发编译数

  默认: 1

- CMAKE_EXTRA_FLAGS: 传递给 cmake 的额外参数

  默认: ‘’

- BUILD_BUNDLED: 从源码编译 thirdparty 依赖，而不是下载预编译包

  默认: OFF

- TCMALLOC_ENABLE: 通过 tcmalloc 的暴露应用的内存信息

  默认: ON

- OPENMLDB_BUILD_TARGET: 只需编译某些target时使用。例如，只想要编译一个测试程序ddl_parser_test，你可以设置`OPENMLDB_BUILD_TARGET=ddl_parser_test`。如果是多个target，用空格隔开。可以减少编译时间，减少编译产出文件，节约存储空间。

  默认: all

### 针对特征工程优化的 OpenMLDB Spark 发行版

[OpenMLDB Spark 发行版](https://github.com/4paradigm/spark)是 [Apache Spark](https://github.com/apache/spark) 的定制发行版。它针对机器学习场景提供特定优化，包括达到10倍到100倍性能提升的原生 LastJoin 实现。你可以使用和标准 Spark 一样的 Java/Scala/Python/SQL 接口，来使用 OpenMLDB Spark 发行版。

注意：为了运行 OpenMLDB 的定制化 SQL 语法，你必须使用该 OpenMLDB Spark 发行版本。

1. 下载预编译的OpenMLDB Spark发行版。

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.6.5/spark-3.2.1-bin-openmldbspark.tgz
```

或者下载源代码并从头开始编译。

```bash
git clone https://github.com/4paradigm/spark.git
cd ./spark/
./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone
```

2. 设置环境变量 `SPARK_HOME` 来使用 OpenMLDB Spark 的发行版本来运行 OpenMLDB 或者其他应用。

```bash
tar xzvf ./spark-3.2.1-bin-openmldbspark.tgz
cd spark-3.2.1-bin-openmldbspark/
export SPARK_HOME=`pwd`
```

3. 你现在可以正常使用 OpenMLDB 了，同时享受由定制化的 Spark 所带来的的性能提升体验。

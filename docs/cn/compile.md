# 编译OpenMLDB

## Linux环境编译
1. 下载代码
    ```bash
    git clone git@github.com:4paradigm/OpenMLDB.git
    cd OpenMLDB
    ```
2. 下载编译所需依赖的 docker 镜像
    ```bash
    docker pull 4pdosc/hybridsql:0.4.0
    ```
3. 启动docker，并绑定本地 OpenMLDB 目录到 docker 中
    ```bash
    docker run -v `pwd`:/OpenMLDB -it 4pdosc/hybridsql:0.4.0 bash
    ```
4. 在 docker 容器内编译 OpenMLDB
    ```bash
    cd /OpenMLDB
    make
    ```

## `make` 额外参数

控制 `make` 的行为. 例如，将默认编译模式改成 Debug:

```bash
make CMAKE_BUILD_TYPE=Debug
```

- OPENMLDB_BUILD_DIR: 代码编译路径

  默认: ${PROJECT_ROOT}/build

- CMAKE_BUILD_TYPE

  默认: RelWithDebInfo

- SQL_PYSDK_ENABLE：是否编译 Python SDK

  默认: OFF

- SQL_JAVASDK_ENABLE：是否编译 Java SDK

  默认: OFF

- TESTING_ENABLE：是否编译测试目标

  默认: OFF

- NPROC: 并发编译数

  默认: $(nproc)

- CMAKE_EXTRA_FLAGS: 传递给 cmake 的额外参数

  默认: ‘’


## 针对OpenMLDB优化的Spark发行版（可选）

[OpenMLDB Spark 发行版](https://github.com/4paradigm/spark)是 [Apache Spark](https://github.com/apache/spark) 的定制发行版。它针对机器学习场景提供特定优化，包括达到10倍到100倍性能提升的原生LastJoin实现。你可以使用和标准Spark一样的Java/Scala/Python/SQL接口，来使用OpenMLDB Spark发行版。

1. 下载预编译的OpenMLDB Spark发行版。

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb0.2.3/spark-3.0.0-bin-openmldbspark.tgz
```

或者下载源代码并从头开始编译。

```bash
git clone https://github.com/4paradigm/spark.git
cd ./spark/
./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone
```

2. 设置环境变量 `SPARK_HOME` 来使用 OpenMLDB Spark 的发行版本来运行 OpenMLDB 或者其他应用。

```bash
tar xzvf ./spark-3.0.0-bin-openmldbspark.tgz
cd spark-3.0.0-bin-openmldbspark/
export SPARK_HOME=`pwd`
```

3. 你现在可以正常使用 OpenMLDB 了，同时享受由定制化的 Spark 所带来的的性能提升体验。

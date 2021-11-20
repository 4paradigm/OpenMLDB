# 编译OpenMLDB

## Linux环境编译
1. 下载代码
    ```
    git clone git@github.com:4paradigm/OpenMLDB.git
    cd OpenMLDB
    ```
2. 下载docker镜像
    ```
    docker pull ghcr.io/4paradigm/hybridsql:0.4.0
    ```
3. 启动docker，并绑定本地OpenMLDB目录到docker中
    ```
    docker run -v `pwd`:/OpenMLDB -it ghcr.io/4paradigm/hybridsql:0.4.0 bash
    ```
4. 编译OpenMLDB
    ```
    make
    ```

## make 额外参数

控制 `make` 的行为. 例如，将默认编译模式改成 Debug:

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

  Default: OFF


## 针对OpenMLDB优化的Spark发行版（可选）

[OpenMLDB Spark发行版](https://github.com/4paradigm/spark)是[Apache Spark](https://github.com/apache/spark)的定制发行版。它针对机器学习场景提供特定优化，包括达到10倍到100倍性能提升的原生LastJoin实现。你可以使用和标准Spark一样的Java/Scala/Python/SQL接口，来使用OpenMLDB Spark发行版。

下载预编译的OpenMLDB Spark发行版。

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb0.2.3/spark-3.0.0-bin-openmldbspark.tgz
```

或者下载源代码并从头开始编译。

```bash
git clone https://github.com/4paradigm/spark.git

cd ./spark/

./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone
```

然后像使用标准Spark一样使用PySpark或SparkSQL。

```bash
tar xzvf ./spark-3.0.0-bin-openmldbspark.tgz

cd spark-3.0.0-bin-openmldbspark/

export SPARK_HOME=`pwd`
```

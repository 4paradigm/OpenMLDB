# 面向特征工程优化的OpenMLDB Spark发行版

## 简介

OpenMLDB Spark发行版是面向特征工程进行优化高性能原生Spark版本。OpenMLDB Spark和标准Spark发行版一样提供Scala、Java、Python和R编程接口，用户使用OpenMLDB Spark发行版方法与标准版一致。

GitHub Repo: https://github.com/4paradigm/Spark/

## 下载OpenMLDB Spark发行版

在Github的[Releases页面](https://github.com/4paradigm/Spark/releases)提供了OpenMLDB Spark发行版的下载地址，用户可以直接下载到本地使用。

注意，预编译的OpenMLDB Spark发行版为allinone版本，可以支持Linux和MacOS操作系统，如有特殊需求也可以下载源码重新编译OpenMLDB Spark发行版。

## OpenMLDB Spark配置

OpenMLDB Spark兼容标准的[Spark配置](https://spark.apache.org/docs/latest/configuration.html)，除此之外，还支持新增的配置项，可以更好地利用原生执行引擎的性能优化。

### 新增配置

| 配置项                                      | 说明                       | 默认值                    | 备注                                                         |
| ------------------------------------------- | -------------------------- | ------------------------- | ------------------------------------------------------------ |
| spark.openmldb.window.parallelization        | 是否启动窗口并行计算优化   | false                     | 窗口并行计算可提高集群利用率但增加计算节点                   |
| spark.openmldb.addIndexColumn.method         | 添加索引列方法             | monotonicallyIncreasingId | 可选方法为zipWithUniqueId, zipWithIndex, monotonicallyIncreasingId |
| spark.openmldb.concatjoin.jointype           | 拼接拼表方法               | inner                     | 可选方法为inner, left, last                                  |
| spark.openmldb.enable.native.last.join       | 是否开启NativeLastJoin优化 | true                      | 相比基于LeftJoin实现性能更高                                 |
| spark.openmldb.enable.unsaferow.optimization | 是否开启UnsafeRow内存优化  | false                     | 开启后降低编解码开销，目前部分复杂类型不支持                 |
| spark.openmldb.physical.plan.graphviz.path   | 导出物理计划图片路径       | ""                        | 默认不导出图片文件                                           |

### 使用Example Jars

下载解压后，设置`SPARK_HOME`环境变量，可以直接执行Example Jars中的例子。

```
export SPARK_HOME=`pwd`/spark-3.0.0-bin-openmldbspark/

$SPARK_HOME/bin/spark-submit \
  --master local \
  --class org.apache.spark.examples.sql.SparkSQLExample \
  $SPARK_HOME/examples/jars/spark-examples*.jar
```

注意，SparkSQLExample为标准Spark源码自带的例子，部分SQL例子使用了OpenMLDB Spark优化进行加速，部分DataFrame例子不支持OpenMLDB Spark优化。

### 使用PySpark

下载OpenMLDB Spark发行版后，也可以使用标准的PySpark编写应用，示例代码如下。

```scala
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
 
spark = SparkSession.builder.appName("demo").getOrCreate()
print(spark.version)

schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
])

rows = [
    Row("Andy", 20),
    Row("Berta", 30),
    Row("Joe", 40)
]

spark.createDataFrame(spark.sparkContext.parallelize(rows), schema).createOrReplaceTempView("t1")
spark.sql("SELECT name, age + 1 FROM t1").show()

```

保存源码文件为`openmldbspark_demo.py`后，使用下面命令提交本地运行。

```
${SPARK_HOME}/bin/spark-submit \
    --master=local \
    ./openmldbspark_demo.py
```


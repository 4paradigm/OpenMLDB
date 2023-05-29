# OpenMLDB Spark 发行版

## 简介

OpenMLDB Spark发行版是面向特征工程优化后的高性能原生Spark版本。OpenMLDB Spark和标准Spark发行版一样提供Scala、Java、Python和R编程接口，用户使用OpenMLDB Spark发行版的方法与标准版一致。

GitHub Repo: https://github.com/4paradigm/Spark/

## 下载OpenMLDB Spark发行版

在上述Github仓库的[Releases页面](https://github.com/4paradigm/Spark/releases)提供了OpenMLDB Spark发行版的下载地址，用户可以直接下载到本地使用。

注意，预编译的OpenMLDB Spark发行版为allinone版本，支持Linux和MacOS操作系统，如有特殊需求也可以下载源码重新编译。

## OpenMLDB Spark配置

OpenMLDB Spark兼容标准的[Spark配置](https://spark.apache.org/docs/latest/configuration.html)，除此之外，还支持新增的配置项，可以更好地利用原生执行引擎的性能优化。

### 新增配置

| 配置项                                      | 说明                         | 默认值                    | 备注                                                          |
| ------------------------------------------- |----------------------------| ------------------------- |-------------------------------------------------------------|
| spark.openmldb.window.parallelization        | 是否启动窗口并行计算优化               | false                     | 窗口并行计算可提高集群利用率但会增加计算节点                                      |
| spark.openmldb.addIndexColumn.method         | 添加索引列方法                    | monotonicallyIncreasingId | 可选方法有zipWithUniqueId, zipWithIndex, monotonicallyIncreasingId |
| spark.openmldb.concatjoin.jointype           | 拼接拼表方法                     | inner                     | 可选方法有inner, left, last                                      |
| spark.openmldb.enable.native.last.join       | 是否开启NativeLastJoin优化       | true                      | 相比基于LeftJoin的实现，具有更高性能                                      |
| spark.openmldb.unsaferowopt.enable | 是否开启UnsafeRow内存优化          | false                     | 开启后使用UnsafeRow编码格式，目前部分复杂类型不支持                              |
| spark.openmldb.unsaferowopt.project | Project节点是否开启UnsafeRow内存优化 | false                     | 开启后降低Project节点编解码开销，目前部分复杂类型不支持                             |
| spark.openmldb.unsaferowopt.window | Window节点是否开启UnsafeRow内存优化  | false                     | 开启后降低Window节点编解码开销，目前部分复杂类型不支持                              |
| spark.openmldb.opt.join.spark_expr | Join条件是否开启Spark表达式优化       | true                     | 开启后Join条件计算使用Spark表达式，减少编解码开销，目前部分复杂表达式不支持                  |
| spark.openmldb.physical.plan.graphviz.path   | 导出物理计划图片的路径                | ""                        | 默认不导出图片文件                                                   |

* 如果SQL任务有多个窗口计算并且计算资源足够，推荐开启窗口并行计算优化，提高资源利用率和降低任务运行时间。
* 如果SQL任务中Join条件表达式比较复杂，默认运行失败，推荐关闭Join条件Spark表达式优化，提高任务运行成功率。
* 如果SQL任务中输入表或中间表列数较大，推荐同时开启上表的三个UnsafeRow优化，减少编解码开销和降低任务运行时间。

## 使用

### 使用Example Jars

下载解压后，设置`SPARK_HOME`环境变量，可以直接执行Example Jars中的例子。

```java
export SPARK_HOME=`pwd`/spark-3.2.1-bin-openmldbspark/

$SPARK_HOME/bin/spark-submit/
  --master local \
  --class org.apache.spark.examples.sql.SparkSQLExample \
  $SPARK_HOME/examples/jars/spark-examples*.jar
```

注意，SparkSQLExample为标准Spark源码自带的例子，部分SQL例子使用了OpenMLDB Spark优化进行加速，部分DataFrame例子不支持OpenMLDB Spark优化。

### 使用PySpark

下载OpenMLDB Spark发行版后，也可以使用标准的PySpark编写应用，示例代码如下。

```python
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


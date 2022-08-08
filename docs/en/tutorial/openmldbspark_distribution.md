# OpenMLDB Spark Distribution

## Overview

The OpenMLDB Spark distribution is a high-performance native Spark version optimized for feature engineering. Like the standard Spark distribution, OpenMLDB Spark provides Scala, Java, Python, and R programming interfaces. Users can use the OpenMLDB Spark in the same way as the standard Spark.

GitHub Repo: https://github.com/4paradigm/Spark/

## Download

You can download the OpenMLDB Spark distribution in the [Release page](https://github.com/4paradigm/Spark/releases) of the repository mentioned above.
```{note}
The pre-compiled OpenMLDB Spark distribution is the AllinOne version, which supports Linux and MacOS operating systems. If you have special requirements, you can also download the source code and recompile it.
```

## Configuration

OpenMLDB Spark supports [standard Spark configuration](https://spark.apache.org/docs/latest/configuration.html) new configuration which can take full advantage of the performance optimization based on the native execution engine.

### Special Configuration in OpenMLDB

| Configuration                                          | Function                                                                      | Default Value             | Note                                                                                                                                                  |
|----------------------------------------------|-------------------------------------------------------------------------------|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.openmldb.window.parallelization        | It defines whether enable the parallel computing with WINDOW.                 | false                     | Parallel computing will improve cluster utilization but increase the number of compute nodes at the same time.                                        |
| spark.openmldb.addIndexColumn.method         | It defines the method of adding indexes on columns.                           | monotonicallyIncreasingId | Options are `zipWithUniqueId`, `zipWithIndex`, `monotonicallyIncreasingId`.                                                                           |
| spark.openmldb.concatjoin.jointype           | It defines the method of concate tables.                                      | inner                     | Options are `inner`, `left`, `last`.                                                                                                                  |
| spark.openmldb.enable.native.last.join       | It defines whether enable the NativeLastJoin optimization                     | true                      | It will have higher performance compared with the implementation based on `LEFT JOIN`, if the value is `true`.                                        |
| spark.openmldb.enable.unsaferow.optimization | It defines whether enable the UnsafeRow memory optimization                   | false                     | It will use the UnsafeRow format to encode, if the value is `true`. Too cpmlicated expressions are not supported currently.                           |
| spark.openmldb.opt.unsaferow.project         | It defines whether enable the UnsafeRow memory optimization on PROJECT nodes. | false                     | It will reduce the overhead of encoding and decoding on PROJECT nodes, if the value is `true`. Too cpmlicated expressions are not supported currently. |
| spark.openmldb.opt.unsaferow.window          | It defines whether enable the UnsafeRow memory optimization on WINDOW nodes.  | false                     | It will reduce the overhead of encoding and decoding on WINDOW nodes, if the value is `true`. Too cpmlicated expressions are not supported currently. |
| spark.openmldb.opt.join.spark_expr           | It defines whether use the Spark expression on JOIN clause.                   | true                      | It will use the Spark expression when processing JOIN clause, if the value is `true`. Too cpmlicated expressions are not supported currently.         |
| spark.openmldb.physical.plan.graphviz.path   | It is the path that the physical plan image will be exported to.              | ""                        | Image files are not exported by default.                                                                                                              |

* If there are multiple window computing tasks and enough resources, it is recommended to set `spark.openmldb.window.parallelization=true` in order to improve resource utilization and reduce runtime.
* If the JOIN expression is too complex, the execution may fail by default. It is recommended to set `spark.openmldb.opt.join.spark_expr=false` to ensure the program can run successfully.
* If there are too many columns in input tables or intermediate tables, you are recommended to enable all three optimization techniques related to `UnsafeRow`, in order to reduce the cost of encoding/decoding and improve the efficiency.

## Usage

### Using Example Jars

The examples in the `Example Jars` can be executed directly after you install the OpenMLDB Spark and set the `SPARK_HOME`.

```java
export SPARK_HOME=`pwd`/spark-3.0.0-bin-openmldbspark/

$SPARK_HOME/bin/spark-submit \
  --master local \
  --class org.apache.spark.examples.sql.SparkSQLExample \
  $SPARK_HOME/examples/jars/spark-examples*.jar
```

```{note}
- SparkSQLExample is an example provided with the standard Spark source code. 
- Some SQL examples use OpenMLDB Spark optimization for higher performance. 
- Some DataFrame examples do not support OpenMLDB Spark optimization.
```
### Using PySpark

You can also use standard PySpark instead after downloading, like the example below.

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

After saving the source file as `openmldbspark_demo.py`, you can use the following command to run the script locally.

```
${SPARK_HOME}/bin/spark-submit \
    --master=local \
    ./openmldbspark_demo.py
```


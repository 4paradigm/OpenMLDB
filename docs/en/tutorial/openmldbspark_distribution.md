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
OpenMLDB Spark supports [standard Spark configuration](https://spark.apache.org/docs/latest/configuration.html). Furthermore, it has new configuration that can take full advantage of the performance optimization based on the native execution engine.
### New Configuration of the OpenMLDB Spark Distribution

| Configuration                                          | Function                                                                        | Default Value             | Note                                                                                                                                                                  |
|----------------------------------------------|---------------------------------------------------------------------------------|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.openmldb.window.parallelization        | It defines whether to enable the window parallelization.                        | false                     | Window parallelization can improve the efficiency when there is sufficient computing resource.                                                                        |
| spark.openmldb.addIndexColumn.method         | It defines the method of adding indexes on columns.                             | monotonicallyIncreasingId | Options are `zipWithUniqueId`, `zipWithIndex`, `monotonicallyIncreasingId`.                                                                                           |
| spark.openmldb.concatjoin.jointype           | It defines the method of concatenating tables.                                  | inner                     | Options are `inner`, `left`, `last`.                                                                                                                                  |
| spark.openmldb.enable.native.last.join       | It defines whether to enable the native last join implementation.               | true                      | When the value is `true`, it will have higher performance compared with the implementation based on `LEFT JOIN`.                                                      |
| spark.openmldb.unsaferowopt.enable | It defines whether to enable the UnsafeRow memory optimization                  | false                     | When the value is `true`, it will use the UnsafeRow format for encoding to improve the performance. However, there are known issues when expressions are complicated. |
| spark.openmldb.unsaferowopt.project         | It defines whether to enable the UnsafeRow memory optimization on PROJECT nodes. | false                     | When the value is `true`, it will reduce the overhead of encoding and decoding on PROJECT nodes but there are known issues for complicated expressions.    |
| spark.openmldb.unsaferowopt.window          | It defines whether to enable the UnsafeRow memory optimization on WINDOW nodes. | false                     | When the value is `true`, it will reduce the overhead of encoding and decoding on WINDOW nodes but there are known issues for complicated expressions.           |
| spark.openmldb.opt.join.spark_expr           | It defines whether to use the Spark expression on JOIN clause.                  | true                      | When the value is `true`, it will use the Spark expression when processing JOIN clause. There are known issues when expressions are complicated as well.              |
| spark.openmldb.physical.plan.graphviz.path   | It is the path that the physical plan image will be exported to.                | ""                        | Image files are not exported by default.                                                                                                                              |

* If there are multiple window computing tasks and enough resources, it is recommended to set `spark.openmldb.window.parallelization=true` in order to improve resource utilization and reduce runtime.
* If the JOIN expression is too complicated, the execution may fail by default. It is recommended to set `spark.openmldb.opt.join.spark_expr=false` to ensure the program can run successfully.
* If there are too many columns in input tables or intermediate tables, you are recommended to enable all three optimization techniques related to `UnsafeRow`, in order to reduce the cost of encoding/decoding and improve the efficiency.

## Usage

### Using Example Jars

The examples in the `Example Jars` can be executed directly after you install the OpenMLDB Spark distribution and set the `SPARK_HOME`.

```java
export SPARK_HOME=`pwd`/spark-3.2.1-bin-openmldbspark/

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

After installing the OpenMLDB Spark distribution, you can use the standard PySpark for development.

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


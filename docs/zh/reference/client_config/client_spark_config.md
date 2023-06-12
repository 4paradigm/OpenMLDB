# 客户端Spark配置文件

## 命令行传递Spark高级参数

OpenMLDB离线任务默认使用Spark执行引擎提交，用户可以在TaskManager配置所有任务的Spark高级参数，也可以在客户端配置单次任务的Spark高级参数，更详细的配置可参考[Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)。

如果需要在SQL命令行修改Spark任务高级参数，可以在本地创建ini格式的配置文件，示例如下。

```
[Spark]
spark.driver.extraJavaOptions=-Dfile.encoding=utf-8
spark.executor.extraJavaOptions=-Dfile.encoding=utf-8
spark.driver.cores=1
spark.default.parallelism=1
spark.driver.memory=4g
spark.driver.memoryOverhead=384
spark.driver.memoryOverheadFactor=0.10
spark.shuffle.compress=true
spark.files.maxPartitionBytes=134217728
spark.sql.shuffle.partitions=200
```

以保存文件成`/work/openmldb/bin/spark.conf`为例，在启动SQL命令行时添加`--spark_conf`参数，示例如下。

```
./openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --spark_conf=/work/openmldb/bin/spark.conf
```

如果配置文件不存在或配置有误，提交离线任务时命令行有相应的错误提示。

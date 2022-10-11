# Spark Client Configuration

## Set Spark Parameters For CLI

The offline jobs of OpenMLDB are submitted as Spark jobs. Users can set default Spark parameters in TaskManager or set Spark parameters for each submission, and refer to [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html) for more detailed configurations.

If we want to set Spark parameters in SQL CLI, we can create the ini configuration file just like this.

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

Take this for example if we save the configruation file as `/work/openmldb/bin/spark.conf`, we can start the SQL CLI with the parameter `--spark_conf` just like this.

```
./openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --spark_conf=/work/openmldb/bin/spark.conf
```

If the configuration file does not exist or is incorrect, we will get errors when submiting the offline jobs.
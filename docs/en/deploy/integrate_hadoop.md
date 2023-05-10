# Hadoop Integration

## Introduction

OpenMLDB supports Haddop integration. Users can run massive parallel applications on Yarn and store big data in HDFS for OpenMLDB.

## Configure Yarn for TaskManager

TaskManager can be configured to integrate with Yarn. Here are the related configruations.

| Config | Type | Note |
| ------ | ---- | ---- |
| spark.master | String | Could be `yarn`, `yarn-cluster` or `yarn-client` |
| spark.home | String | The path of Spark home |
| spark.default.conf | String | The config of Spark jobs |

If you are using Yarn mode, the offline jobs will be run in cluster. Users should configure the offline data path in HDFS, othereise the jobs may be read the offline data. Here is the example configuration.

```
offline.data.prefix=hdfs:///foo/bar/
```

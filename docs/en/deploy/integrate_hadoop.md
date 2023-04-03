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

## Retrieve Yarn Logs

OpenMLDB supports command `SHOW JOBLOG` to get the logs of offline jobs. If we are using `local` or `yarn-client` mode, we can get the driver log in real time. If we are using `yarn-cluster` mode, we can get the client log in real time but not the driver log. We can still get the driver log but we have the following limitaions.

* Only support Hadoop 3.x and the version of 3.3.4 has been tested。
* Users should enable log aggreation in Yarn cluster. May config `yarn.log-aggregation-enable` in `core-site.xml`.
* We have to wait for the job to be finished before getting the completed driver log.

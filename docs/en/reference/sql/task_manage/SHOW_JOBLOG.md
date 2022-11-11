# SHOW JOBLOG

The `SHOW JOBLOG` statement is used to display the log of a single job that has been submitted according to the given JOB ID.

```SQL
SHOW JOBLOG job_id;
```


## Example

Submit an online data import task:

```sql
LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');
```
The output is shown below. The job id of the above command is 1.
```sql
---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  id   job_type           state       start_time   end_time        parameter                                                                                                                    cluster   application_id   error
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
  1    ImportOnlineData   Submitted   0            2022-11-02 10:24:01   LOAD DATA INFILE 'file:///tmp/test.csv' INTO TABLE demo_db.t1 options(format='csv', header=false, mode='append');           local
 ---- ------------------ ----------- ------------ --------------- ---------------------------------------------------------------------------------------------------------------------------- --------- ---------------- -------
```

Check the log of job whose Job ID is 1:
```sql
SHOW JOBLOG 1;

Stdout:
+---+
|  1|
+---+
+---+

Stderr:
22/11/02 10:24:01 WARN util.Utils: Your hostname, mbp16.local resolves to a loopback address: 127.0.0.1; using 192.168.102.25 instead (on interface en0)
22/11/02 10:24:01 WARN util.Utils: Set SPARK_LOCAL_IP if you need to bind to another address
22/11/02 10:24:02 INFO spark.SparkContext: Running Spark version 3.0.0
22/11/02 10:24:02 INFO resource.ResourceUtils: ==============================================================
22/11/02 10:24:02 INFO resource.ResourceUtils: Resources for spark.driver:

22/11/02 10:24:02 INFO resource.ResourceUtils: ==============================================================
22/11/02 10:24:02 INFO spark.SparkContext: Submitted application: com._4paradigm.openmldb.batchjob.RunBatchAndShow
22/11/02 10:24:02 INFO spark.SecurityManager: Changing view acls to: tobe,hdfs
22/11/02 10:24:02 INFO spark.SecurityManager: Changing modify acls to: tobe,hdfs
22/11/02 10:24:02 INFO spark.SecurityManager: Changing view acls groups to:
22/11/02 10:24:02 INFO spark.SecurityManager: Changing modify acls groups to:
22/11/02 10:24:02 INFO spark.SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(tobe, hdfs); groups with view permissions: Set(); users  with modify permissions: Set(tobe, hdfs); groups with modify permissions: Set()
22/11/02 10:24:02 INFO util.Utils: Successfully started service 'sparkDriver' on port 62893.
22/11/02 10:24:02 INFO spark.SparkEnv: Registering MapOutputTracker
22/11/02 10:24:02 INFO spark.SparkEnv: Registering BlockManagerMaster
22/11/02 10:24:02 INFO storage.BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information

...

22/11/02 10:24:07 INFO zookeeper.ZooKeeper: Initiating client connection, connectString=0.0.0.0:2181 sessionTimeout=5000 watcher=org.apache.curator.ConnectionState@383cb5ce
22/11/02 10:24:07 INFO zookeeper.ClientCnxn: Opening socket connection to server 0.0.0.0/0.0.0.0:2181. Will not attempt to authenticate using SASL (unknown error)22/11/02 10:24:07 INFO zookeeper.ClientCnxn: Socket connection established to 0.0.0.0/0.0.0.0:2181, initiating session
22/11/02 10:24:07 INFO zookeeper.ClientCnxn: Session establishment complete on server 0.0.0.0/0.0.0.0:2181, sessionid = 0x100037191930031, negotiated timeout = 5000
22/11/02 10:24:07 INFO state.ConnectionStateManager: State change: CONNECTED
22/11/02 10:24:07 INFO batch.SparkPlanner: Disable window parallelization optimization, enable by setting openmldb.window.parallelization
22/11/02 10:24:07 INFO sdk.SqlEngine: Dumped module size: 1003
22/11/02 10:24:07 INFO batch.SparkPlanner: Visit physical plan to find ConcatJoin node
22/11/02 10:24:07 INFO batch.SparkPlanner: Visit concat join node to add node index info
22/11/02 10:24:08 INFO server.AbstractConnector: Stopped Spark@7af1cd63{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
22/11/02 10:24:08 INFO ui.SparkUI: Stopped Spark web UI at http://192.168.102.25:4040
22/11/02 10:24:08 INFO spark.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
22/11/02 10:24:08 INFO memory.MemoryStore: MemoryStore cleared
22/11/02 10:24:08 INFO storage.BlockManager: BlockManager stopped
22/11/02 10:24:08 INFO storage.BlockManagerMaster: BlockManagerMaster stopped
22/11/02 10:24:08 INFO scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
22/11/02 10:24:08 INFO spark.SparkContext: Successfully stopped SparkContext
22/11/02 10:24:08 INFO util.ShutdownHookManager: Shutdown hook called
22/11/02 10:24:08 INFO util.ShutdownHookManager: Deleting directory /private/var/folders/jz/df2p7wt50fd40w2y9kkm05h40000gn/T/spark-3bda5a3e-7777-497d-9a33-0e7b1b0bafb3
22/11/02 10:24:08 INFO util.ShutdownHookManager: Deleting directory /private/var/folders/jz/df2p7wt50fd40w2y9kkm05h40000gn/T/spark-9f4ff94e-5cb7-4a78-a586-5081231da246
```

## Related Sentences

[SHOW JOBS](./SHOW_JOBS.md)

[STOP JOBS](./STOP_JOB.md)

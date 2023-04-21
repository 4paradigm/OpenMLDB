/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.batch

import com._4paradigm.openmldb.batch.utils.{ConfigOption, ConfigReflections}
import org.apache.spark.sql.SparkSession


class OpenmldbBatchConfig extends Serializable {

  @ConfigOption(name = "openmldb.print.version", doc = "Print the OpenMLDB version or not")
  var printVersion: Boolean = true

  // The integration like WindowAgg and GroupBy will use this config to set partition number
  @ConfigOption(name = "openmldb.groupby.partitions", doc = "Default partition number used in group by")
  var groupbyPartitions: Int = -1

  @ConfigOption(name = "spark.sql.session.timeZone")
  var timeZone = "Asia/Shanghai"

  // test mode
  @ConfigOption(name = "openmldb.test.tiny", doc = "控制读取表的数据条数，默认读全量数据")
  var tinyData: Long = -1

  @ConfigOption(name = "openmldb.test.print.sampleInterval", doc = "每隔N行打印一次数据信息")
  var printSampleInterval: Long = 100 * 100

  @ConfigOption(name = "openmldb.test.print.printContent", doc = "打印完整行内容")
  var printRowContent = false

  @ConfigOption(name = "openmldb.test.print", doc = "执行过程中允许打印数据")
  var print: Boolean = false

  // Debug options
  @ConfigOption(name = "openmldb.debug.show_node_df", doc = "Use Spark DataFrame.show() for each physical nodes")
  var debugShowNodeDf: Boolean = false

  // Window skew optimization
  @ConfigOption(name = "openmldb.window.skew.opt", doc = "Enable window skew optimization or not")
  var enableWindowSkewOpt: Boolean = false

  @ConfigOption(name = "openmldb.window.skew.opt.broadcastJoin", doc = "Window skew optimization will cache reused " +
    "tables")
  var windowSkewOptBroadcastJoin: Boolean = true

  @ConfigOption(name = "openmldb.window.skew.expanded.all.opt",
    doc = "Enable window skew optimization expanded all data")
  var enableWindowSkewExpandedAllOpt: Boolean = true

  @ConfigOption(name = "openmldb.window.skew.opt.postfix", doc = "The postfix for internal tables and columns")
  var windowSkewOptPostfix = ""

  @ConfigOption(name = "openmldb.skew.partition.num", doc = "The num of partition for repartition")
  var skewedPartitionNum: Int = 2

  @ConfigOption(name = "openmldb.window.skew.opt.cache", doc = "Window skew optimization will cache reused tables")
  var windowSkewOptCache: Boolean = true

  @ConfigOption(name = "openmldb.window.skew.opt.config", doc = "The skew config for window skew optimization")
  var windowSkewOptConfig: String = ""

  @ConfigOption(name = "openmldb.slowRunCacheDir", doc =
    """
      | Slow run mode cache directory path. If specified, run OpenMLDB plan with slow mode.
      | The plan operations will run by slow steps. Each step run single operation only.
      | It will load dependent steps' output data from disk, and store it's output data
      | to disk also.""")
  var slowRunCacheDir: String = _

  // 窗口数据采样
  @ConfigOption(name = "openmldb.window.sampleMinSize", doc = "Minimum window size to trigger sample dumping")
  var windowSampleMinSize: Int = -1

  @ConfigOption(name = "openmldb.window.sampleOutputPath", doc = "Window sample output path")
  var windowSampleOutputPath: String = _

  @ConfigOption(name = "openmldb.window.parallelization", doc = "Enable window compute parallelization optimization")
  var enableWindowParallelization: Boolean = false

  @ConfigOption(name = "openmldb.window.sampleFilter", doc =
    """
      | Filter condition for window sample, currently only support simple equalities
      | like "col1=123, col2=456" etc.
    """)
  var windowSampleFilter: String = _

  @ConfigOption(name = "openmldb.window.sampleBeforeCompute", doc = "Dump sample before window computation")
  var windowSampleBeforeCompute: Boolean = false

  @ConfigOption(name = "openmldb.window.sampleLimit", doc = "Maximum sample to dump for each partition")
  var windowSampleLimit = 10

  @ConfigOption(name = "openmldb.addIndexColumn.method",
    doc = "The method to add index column(zipWithUniqueId, zipWithIndex, monotonicallyIncreasingId")
  var addIndexColumnMethod = "monotonicallyIncreasingId"

  @ConfigOption(name = "openmldb.concatjoin.jointype",
    doc = "The join type type for concat join(inner, left, last)")
  var concatJoinJoinType = "inner"

  @ConfigOption(name = "openmldb.physical.plan.graphviz.path", doc = "The path of physical plan graphviz image")
  var physicalPlanGraphvizPath = ""

  @ConfigOption(name = "openmldb.debug.print_physical_plan", doc = "Print the sql physical plan")
  var printPhysicalPlan = false

  @ConfigOption(name = "openmldb.enable.native.last.join", doc = "Enable native last join or not")
  var enableNativeLastJoin = false

  // UnsafeRow optimization
  @ConfigOption(name = "openmldb.unsaferowopt.enable", doc = "Enable UnsafeRow optimization or not")
  var enableUnsafeRowOptimization = false

  @ConfigOption(name = "openmldb.unsaferowopt.project", doc = "Enable UnsafeRow optimization for project")
  var enableUnsafeRowOptForProject = true

  @ConfigOption(name = "openmldb.unsaferowopt.window", doc = "Enable UnsafeRow optimization for window")
  var enableUnsafeRowOptForWindow = true

  //@ConfigOption(name = "openmldb.opt.unsaferow.groupby", doc = "Enable UnsafeRow optimization for groupby")
  //var enableUnsafeRowOptForGroupby = false

  @ConfigOption(name = "openmldb.unsaferowopt.copydirectbytebuffer", doc = "Copy row with DirectByteBuffer")
  var unsaferowoptCopyDirectByteBuffer = false

  // Join optimization
  @ConfigOption(name = "openmldb.opt.join.spark_expr", doc = "Enable join with original Spark expression")
  var enableJoinWithSparkExpr = true

  // Use SparkSQL
  @ConfigOption(name = "openmldb.sparksql", doc = "Enable SparkSQL instead of using OpenMLDB execution engine")
  var enableSparksql = false

  // OpenMLDB Java SDK dynamic library path, notice that this should not be set hybridse jsdk so
  @ConfigOption(name = "openmldb.jsdk.library.path", doc = "The path of OpenMLDB Java SDK core file path")
  var openmldbJsdkLibraryPath = ""

  @ConfigOption(name = "openmldb.zk.cluster", doc = "The cluster of ZooKeeper for NameServer")
  var openmldbZkCluster = ""

  @ConfigOption(name = "openmldb.zk.root.path", doc = "The root path of ZooKeeper for NameServer")
  var openmldbZkRootPath = ""

  @ConfigOption(name = "openmldb.default.db", doc = "The default database for OpenMLDB SQL")
  var defaultDb = "default_db"

  @ConfigOption(name = "openmldb.loaddata.mode", doc = "The mode to choose target storage: online/offline")
  var loadDataMode = "offline"

  @ConfigOption(name = "openmldb.offline.data.prefix", doc = "The prefix of offline data")
  var offlineDataPrefix = "file:///tmp/openmldb_offline/"

  @ConfigOption(name = "openmldb.taskmanager.external.function.dir", doc = "The absolute path of TaskManager external" +
    " function dir")
  var taskmanagerExternalFunctionDir = "/tmp/udf/"

  @ConfigOption(name = "openmldb.savejobresult.http", doc = "The http url of JobResultSaver(taskmanager), " +
    "send df to it")
  var saveJobResultHttp = ""

  @ConfigOption(name = "openmldb.savejobresult.resultid", doc = "The savejobresult id")
  var saveJobResultId = ""

  // If a post req is too large, > brpc max_body_size, it'll be reject, default is 64M.
  @ConfigOption(name = "openmldb.savejobresult.rowperpost", doc = "The max row count of a http post request to " +
    "savejobresult, default is 16000, if the row count is larger than this, will split it into multiple http request" +
    " with same resultid and different row")
  var saveJobResultRowPerPost = 16000

  @ConfigOption(name = "openmldb.savejobresult.posttimeouts", doc = "ConnectionRequestTimeout,ConnectTimeout," +
    "SocketTimeout for http post request to savejobresult, default is '10000,10000,10000', unit is ms")
  var saveJobResultPostTimeouts = "10000,10000,10000"

  @ConfigOption(name = "openmldb.aws.access_key", doc = "AWS access key")
  var awsAccessKey = ""

  @ConfigOption(name = "openmldb.aws.secret_key", doc = "AWS secret key")
  var awsSecretKey = ""

  @ConfigOption(name = "openmldb.aws.region", doc = "AWS region")
  var awsRegion = ""
}

object OpenmldbBatchConfig {

  def fromSparkSession(sess: SparkSession): OpenmldbBatchConfig = {
    ConfigReflections.createConfig(sess.conf)((conf, name) => {
      conf.getOption(name).orElse(conf.getOption("spark." + name)).orNull
    })
  }

  def fromDict(dict: Map[String, Any]): OpenmldbBatchConfig = {
    ConfigReflections.createConfig(dict)((d, name) => {
      d.getOrElse(name, null)
    })
  }

}

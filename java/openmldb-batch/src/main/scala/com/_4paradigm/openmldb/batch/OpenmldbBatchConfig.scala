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

  // The integration like WindowAgg and GroupBy will use this config to set partition number
  @ConfigOption(name="openmldb.groupby.partitions", doc="Default partition number used in group by")
  var groupbyPartitions: Int = -1

  @ConfigOption(name="openmldb.dbName", doc="Database name")
  var configDBName = "spark_db"

  @ConfigOption(name="spark.sql.session.timeZone")
  var timeZone = "Asia/Shanghai"

  // test mode 用于测试的时候验证相关问题
  @ConfigOption(name="openmldb.test.tiny", doc="控制读取表的数据条数，默认读全量数据")
  var tinyData: Long = -1

  @ConfigOption(name="openmldb.test.print.sampleInterval", doc="每隔N行打印一次数据信息")
  var printSampleInterval: Long = 100 * 100

  @ConfigOption(name="openmldb.test.print.printContent", doc="打印完整行内容")
  var printRowContent = false

  @ConfigOption(name="openmldb.test.print", doc="执行过程中允许打印数据")
  var print: Boolean = false

  // Window skew optimization
  @ConfigOption(name="openmldb.window.skew.opt", doc="Enable window skew optimization or not")
  var enableWindowSkewOpt: Boolean = false

  @ConfigOption(name="openmldb.window.skew.opt.postfix", doc="The postfix for internal tables and columns")
  var windowSkewOptPostfix = ""

  @ConfigOption(name="openmldb.skew.watershed", doc="针对key的分水岭，默认是100*100条，才做数据倾斜优化")
  var skewCnt: Int = 2

  @ConfigOption(name="openmldb.skew.level", doc="""
      | 数据倾斜优化级别，默认是1，数据拆分两份分别计算，优化1倍。
      | 因为数据按照2的n次方拆分。所以不建议level改太大""")
  var skewLevel: Int = 1

  @ConfigOption(name="openmldb.window.skew.opt.cache", doc="Window skew optimization will cache reused tables")
  var windowSkewOptCache: Boolean = true

  @ConfigOption(name="openmldb.window.skew.opt.config", doc="The skew config for window skew optimization")
  var windowSkewOptConfig: String = ""

  // 慢速执行模式
  @ConfigOption(name="openmldb.slowRunCacheDir", doc="""
      | Slow run mode cache directory path. If specified, run OpenMLDB plan with slow mode.
      | The plan operations will run by slow steps. Each step run single operation only.
      | It will load dependent steps' output data from disk, and store it's output data
      | to disk also.""")
  var slowRunCacheDir: String = _

  // 窗口数据采样
  @ConfigOption(name="openmldb.window.sampleMinSize", doc="Minimum window size to trigger sample dumping")
  var windowSampleMinSize: Int = -1

  @ConfigOption(name="openmldb.window.sampleOutputPath", doc="Window sample output path")
  var windowSampleOutputPath: String = _

  @ConfigOption(name="openmldb.window.parallelization", doc="Enable window compute parallelization optimization")
  var enableWindowParallelization: Boolean = false

  @ConfigOption(name="openmldb.window.sampleFilter", doc="""
      | Filter condition for window sample, currently only support simple equalities
      | like "col1=123, col2=456" etc.
    """)
  var windowSampleFilter: String = _

  @ConfigOption(name="openmldb.window.sampleBeforeCompute", doc="Dump sample before window computation")
  var windowSampleBeforeCompute: Boolean = false

  @ConfigOption(name="openmldb.window.sampleLimit", doc="Maximum sample to dump for each partition")
  var windowSampleLimit = 10

  @ConfigOption(name="openmldb.addIndexColumn.method",
    doc="The method to add index column(zipWithUniqueId, zipWithIndex, monotonicallyIncreasingId")
  var addIndexColumnMethod = "monotonicallyIncreasingId"

  @ConfigOption(name="openmldb.concatjoin.jointype",
    doc="The join type type for concat join(inner, left, last)")
  var concatJoinJoinType = "inner"

  @ConfigOption(name="openmldb.physical.plan.graphviz.path", doc="The path of physical plan graphviz image")
  var physicalPlanGraphvizPath = ""

  @ConfigOption(name="openmldb.enable.native.last.join", doc="Enable native last join or not")
  var enableNativeLastJoin = true

  // UnsafeRow optimization
  @ConfigOption(name="openmldb.unsaferow.opt", doc="Enable UnsafeRow optimization or not")
  var enableUnsafeRowOptimization = false

  // Switch for disable OpenMLDB
  @ConfigOption(name="openmldb.disable", doc="Disable OpenMLDB optimization or not")
  var disableOpenmldb = false

  // HybridSE dynamic library path
  // TODO: Support this when upgrading hybridse-sdk to 0.1.4
  //@ConfigOption(name="openmldb.hybridse.jsdk.path", doc="The path of HybridSE jsdk core file path")
  var hybridseJsdkLibraryPath = ""

  @ConfigOption("openmldb.enable.hive.metastore", "Need to set hive.metastore.uris")
  var enableHiveMetaStore = false

  @ConfigOption("openmldb.hive.default.database", "The default database from hive metastore")
  var defaultHiveDatabase = "default"

  @ConfigOption(name="openmldb.hadoop.warehouse.path", doc="The path of Hadoop warehouse")
  var hadoopWarehousePath = ""

  @ConfigOption(name="openmldb.iceberg.catalog.name", doc="The name of Iceberg catalog")
  val icebergHadoopCatalogName = "iceberg_catalog"

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

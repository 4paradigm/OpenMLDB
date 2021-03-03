/*
 * FeSQLConfig.scala
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

package com._4paradigm.fesql.spark

import com._4paradigm.fesql.spark.utils.{ConfigOption, ConfigReflections}
import org.apache.spark.sql.SparkSession


class FeSQLConfig extends Serializable {

  @ConfigOption(name="fesql.group.partitions", doc="Default partition number used in group by")
  var groupPartitions: Int = 0

  @ConfigOption(name="fesql.dbName", doc="Database name")
  var configDBName = "spark_db"

  @ConfigOption(name="spark.sql.session.timeZone")
  var timeZone = "Asia/Shanghai"

  // test mode 用于测试的时候验证相关问题
  @ConfigOption(name="fesql.test.tiny", doc="控制读取表的数据条数，默认读全量数据")
  var tinyData: Long = -1

  @ConfigOption(name="fesql.test.print.sampleInterval", doc="每隔N行打印一次数据信息")
  var printSampleInterval: Long = 100 * 100

  @ConfigOption(name="fesql.test.print.printContent", doc="打印完整行内容")
  var printRowContent = false

  @ConfigOption(name="fesql.test.print", doc="执行过程中允许打印数据")
  var print: Boolean = false

  // 数据倾斜优化
  @ConfigOption(name="fesql.mode", doc="默认normal模式, 如果需要针对数据倾斜优化，需要改成skew")
  var skewMode: String = "normal"

  @ConfigOption(name="fesql.skew.cnt.name", doc="倾斜条数字段名")
  var skewCntName = "key_cnt_4paradigm"

  @ConfigOption(name="fesql.skew.tag.name", doc="tag字段名")
  var skewTag = "tag_4paradigm"

  @ConfigOption(name="fesql.skew.position.name", doc="position字段名")
  var skewPosition = "position_4paradigm"

  @ConfigOption(name="fesql.skew.watershed", doc="针对key的分水岭，默认是100*100条，才做数据倾斜优化")
  var skewCnt: Int = 100

  @ConfigOption(name="fesql.skew.ratio", doc="""
      | 数据倾斜因子，如果某个key占总数据量比例一半，那么认为需要优化。范围是0 ~ 1""")
  var skewRatio: Double = 0.5

  @ConfigOption(name="fesql.skew.level", doc="""
      | 数据倾斜优化级别，默认是1，数据拆分两份分别计算，优化1倍。
      | 因为数据按照2的n次方拆分。所以不建议level改太大""")
  var skewLevel: Int = 1

  // 慢速执行模式
  @ConfigOption(name="fesql.slowRunCacheDir", doc="""
      | Slow run mode cache directory path. If specified, run spark plan with slow mode.
      | The plan operations will run by slow steps. Each step run single operation only.
      | It will load dependent steps' output data from disk, and store it's output data
      | to disk also.""")
  var slowRunCacheDir: String = _

  // 窗口数据采样
  @ConfigOption(name="fesql.window.sampleMinSize", doc="Minimum window size to trigger sample dumping")
  var windowSampleMinSize: Int = -1

  @ConfigOption(name="fesql.window.sampleOutputPath", doc="Window sample output path")
  var windowSampleOutputPath: String = _

  @ConfigOption(name="fesql.window.parallelization", doc="Enable window compute parallelization optimization")
  var enableWindowParallelization: Boolean = false

  @ConfigOption(name="fesql.window.sampleFilter", doc="""
      | Filter condition for window sample, currently only support simple equalities
      | like "col1=123, col2=456" etc.
    """)
  var windowSampleFilter: String = _

  @ConfigOption(name="fesql.window.sampleBeforeCompute", doc="Dump sample before window computation")
  var windowSampleBeforeCompute: Boolean = false

  @ConfigOption(name="fesql.window.sampleLimit", doc="Maximum sample to dump for each partition")
  var windowSampleLimit = 10

  @ConfigOption(name="fesql.addIndexColumn.method", doc="The method to add index column(zipWithUniqueId, zipWithIndex, monotonicallyIncreasingId")
  var addIndexColumnMethod = "monotonicallyIncreasingId"

  @ConfigOption(name="fesql.concatjoin.jointype", doc="The join type type for concat join(innerjoin, leftjoin, lastjoin)")
  var concatJoinJoinType = "inner"

  @ConfigOption(name="fesql.physical.plan.graphviz.path", doc="The path of physical plan graphviz image")
  var physicalPlanGraphvizPath = ""

  @ConfigOption(name="fesql.enable.native.last.join", doc="Enable native last join or not")
  var enableNativeLastJoin = true

}


object FeSQLConfig {

  // 数据倾斜优化
  final val SKEW = "skew"

  def fromSparkSession(sess: SparkSession): FeSQLConfig = {
    ConfigReflections.createConfig(sess.conf)((conf, name) => {
      conf.getOption(name).orElse(conf.getOption("spark." + name)).orNull
    })
  }

  def fromDict(dict: Map[String, Any]): FeSQLConfig = {
    ConfigReflections.createConfig(dict)((d, name) => {
      d.getOrElse(name, null)
    })
  }
}

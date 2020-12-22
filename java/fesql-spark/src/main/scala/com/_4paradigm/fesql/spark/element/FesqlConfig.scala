package com._4paradigm.fesql.spark.element

//import com._4paradigm.fesql.utils.SkewUtils

object FesqlConfig {
  // 配置名字
  final val configPartitions = "spark.fesql.group.partitions"
  final val configTimeZone = "spark.sql.session.timeZone"
  final val configFesqlTZ = "spark.fesql.timeZone"
  final val configMode =  "spark.fesql.mode"
  // ================================================
  // skew mode
  final val configSkewRadio = "spark.fesql.skew.ratio"
  final val configSkewLevel = "spark.fesql.skew.level"
  // 针对key的分水岭，默认是100 * 100 条，才做数据倾斜优化
  final val configSkewCnt = "spark.fesql.skew.watershed"
  final val configSkewCntName = "spark.fesql.skew.cnt.name"
  final val configSkewTag = "spark.fesql.skew.tag"
  final val configSkewPosition = "spark.fesql.skew.position"

  // ================================================
  // test mode 用于测试的时候验证相关问题
  // 控制读取表的数据条数，默认读全量数据
  final val configTinyData = "spark.fesql.test.tiny"
  // ================================================

  // enable spark2.3.0 service
  final val configSparkEnable = "spark.hadoop.yarn.timeline-service.enabled"
  var configDBName = "spark_db"






  // 配置的默认值
  var paritions: Int = 0
  var timeZone = "Asia/Shanghai"
  // 默认normal模式
  // 如果需要针对数据倾斜优化，就要给出 skew
  var mode = "normal"
  // ================================================
  // skew mode
  // 数据倾斜因子，如果某个key占总数据量比例一半，那么认为需要优化。范围是0 ~ 1
  var skewRatio: Double = 0.5
  // 优化级别，默认是1，数据拆分两份分别计算，优化1倍。因为数据按照2的n次方拆分。所以不建议level改太大
  var skewLevel: Int = 1
  var skewCnt: Int = 100
  // 条数字段名
  var skewCntName = "key_cnt_4paradigm"
  // tag字段名
  var skewTag = "tag_4paradigm"
  // position字段名
  var skewPosition = "position_4paradigm"
  // ================================================
  // test mode 用于测试的时候验证相关问题
  // 控制读取表的数据条数，-1表示读全量数据
  var tinyData: Long = -1
  // ================================================
  // 常量区
  final val skew = "skew"

}

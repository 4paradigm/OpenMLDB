package com._4paradigm.fesql.spark.element

//import com._4paradigm.fesql.utils.SkewUtils

object FesqlConfig {
  // 配置名字
  val configPartitions = "fesql.group.partitions"
  val configTimeZone = "spark.sql.session.timeZone"
  val configFesqlTZ = "fesql.timeZone"
  val configMode =  "fesql.mode"
  // ================================================
  // skew mode
  val configSkewRadio = "fesql.skew.ratio"
  val configSkewLevel = "fesql.skew.level"
  // 针对key的分水岭，默认是100 * 100 条，才做数据倾斜优化
  val configSkewCnt = "fesql.skew.watershed"
  val configSkewCntName = "fesql.skew.cnt.name"
  val configSkewTag = "fesql.skew.tag"
  val configSkewPosition = "fesql.skew.position"

  // ================================================
  // enable spark2.3.0 service
  val configSparkEnable = "spark.hadoop.yarn.timeline-service.enabled"
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
  var skewCnt: Int = 100 * 100
  // 条数字段名
  var skewCntName = "key_cnt_wzx"
  // tag字段名
  var skewTag = "tag_wzx"
  // position字段名
  var skewPosition = "position_wzx"
  // ================================================
  // 常量区
  val skew = "skew"

}

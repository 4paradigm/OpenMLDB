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

package com._4paradigm.openmldb.batch.window

import java.io.{IOException, ObjectInputStream, ObjectOutputStream, OutputStream, PrintStream}
import com._4paradigm.hybridse.codec.{Row => NativeRow}
import com._4paradigm.hybridse.sdk.JitManager
import com._4paradigm.hybridse.vm.{CoreAPI, HybridSeJitWrapper}
import com._4paradigm.openmldb.batch.OpenmldbBatchConfig
import com._4paradigm.openmldb.batch.window.WindowAggPlanUtil.WindowAggConfig
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, TimestampType}
import org.slf4j.LoggerFactory


class WindowSampleSupport(fs: FileSystem,
                          partitionIndex: Int,
                          config: WindowAggConfig,
                          sqlConfig: OpenmldbBatchConfig,
                          jit: HybridSeJitWrapper) extends WindowHook {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val minWindowSize = sqlConfig.windowSampleMinSize
  private val outputPath = sqlConfig.windowSampleOutputPath + "/" + config.windowName + "/" + partitionIndex
  private val dumpBeforeCompute = sqlConfig.windowSampleBeforeCompute
  private val sampleLimit = sqlConfig.windowSampleLimit
  private val conditions = parseFilterCondition(sqlConfig.windowSampleFilter)

  private var sampledCnt = 0

  override def preCompute(computer: WindowComputer, row: Row): Unit = {
    if (dumpBeforeCompute && shouldSample(computer, row)) {
      doDump(computer, row)
    }
  }

  override def postCompute(computer: WindowComputer, row: Row): Unit = {
    if (!dumpBeforeCompute && shouldSample(computer, row)) {
      doDump(computer, null)
    }
  }

  private def doDump(computer: WindowComputer, row: Row): Unit = {
    if (sampledCnt == 0) {
      dumpModule()
      dumpConfig()
    }
    dumpWindow(row, computer, sampledCnt.toString)
    sampledCnt += 1
  }

  private def shouldSample(computer: WindowComputer, row: Row): Boolean = {
    sampledCnt < sampleLimit &&
      computer.getWindow.size() >= minWindowSize &&
      matchFilterCondition(row)
  }

  private def parseFilterCondition(desc: String): Array[(Int, String)] = {
    if (desc == null || desc.isEmpty) {
      return Array.empty
    }
    val parts = desc.split(",").map(_.trim)
    parts.flatMap(t => {
      val idx = t.indexOf("=")
      if (idx < 0) {
        None
      } else {
        val colName = t.substring(0, idx)
        var value = t.substring(idx + 1)
        if (value.isEmpty) {
          value = null
        } else if (value.startsWith("\"") && value.endsWith("\"")) {
          value = value.substring(1, value.length - 1)
        }
        val colIdx = config.inputSchema.indexWhere(_.name == colName)
        if (colIdx < 0) {
          None
        } else {
          Some((colIdx, value))
        }
      }
    })
  }

  private def matchFilterCondition(row: Row): Boolean = {
    conditions.forall { case (idx, str) =>
      str == formatFieldString(row, idx, config.inputSchema(idx).dataType)
    }
  }

  private def formatFieldString(row: Row, idx: Int, dtype: DataType): String = {
    if (idx >= row.size || row.isNullAt(idx)) {
      return null
    }
    dtype match {
      case BooleanType => row.getBoolean(idx).toString
      case ShortType => row.getShort(idx).toString
      case IntegerType => row.getInt(idx).toString
      case LongType => row.getLong(idx).toString
      case FloatType => row.getFloat(idx).toString
      case DoubleType => row.getDouble(idx).toString
      case TimestampType => row.getTimestamp(idx).getTime.toString
      case DateType => row.getDate(idx).toString
      case StringType => row.getString(idx)
      case _ =>
        logger.warn(s"Do not know how to format $dtype")
        row.get(idx).toString
    }
  }

  private def dumpModule(): Unit = {
    val buffer = config.moduleNoneBroadcast.getBuffer
    val bytes = new Array[Byte](buffer.capacity())
    buffer.get(bytes)
    dumpToFile(outputPath + "/module.ll", stream => stream.write(bytes))
  }

  private def dumpConfig(): Unit = {
    dumpToFile(outputPath + "/config.obj", stream => {
      val ostream = new ObjectOutputStream(stream)
      ostream.writeObject(config)
      ostream.flush()
    })
  }

  private def dumpWindow(curRow: Row, computer: WindowComputer, desc: String): Unit = {
    val window = computer.getWindow
    val fieldNum = config.inputSchema.size
    val rowNum = if (curRow != null) window.size() + 1 else window.size()
    val rows = new Array[Array[Any]](rowNum.toInt)

    var i = 0
    if (curRow != null) {
      rows(i) = curRow.toSeq.toArray
      i += 1
    }
    while (i < rowNum) {
      val row = window.Get(i)
      val arr = new Array[Any](fieldNum)
      computer.getEncoder.decode(row, arr)
      rows(i) = arr
      i += 1
    }

    dumpToFile(outputPath + "/window_" + desc + ".obj", stream => {
      val ostream = new ObjectOutputStream(stream)
      ostream.writeObject(rows)
      ostream.flush()
    })

    dumpToFile(outputPath + "/window_" + desc + ".txt", stream => {
      val ps = new PrintStream(stream)
      ps.println(config.inputSchema.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(","))
      rows.foreach(row => ps.println(row.mkString(",")))
      ps.flush()
    })
  }

  private def dumpToFile(path: String, write: OutputStream => Unit): Unit = {
    try {
      val file = fs.create(new Path(path), true)
      try {
        write(file)
      } catch {
        case e: IOException =>
          logger.error(s"Write to $path failed", e)
          e.printStackTrace()
      } finally {
        file.close()
      }
    } catch {
      case e: IOException =>
        logger.error(s"Create file $path failed", e)
        e.printStackTrace()
    }
  }
}


object WindowSampleSupport {

  class SampleExecutor(sqlConfig: OpenmldbBatchConfig, val config: WindowAggConfig) {

    var javaData: Array[Array[Any]] = _

    private val jit = {
      val buffer = config.moduleNoneBroadcast.getBuffer
      SqlClusterExecutor.initJavaSdkLibrary(sqlConfig.openmldbJsdkLibraryPath)
      JitManager.initJitModule(config.moduleTag, buffer)
      JitManager.getJit(config.moduleTag)
    }

    // TODO: Check if we need to support keep index column here
    private val computer = new WindowComputer(config, jit, false)

    private var curRow: Row = _
    private var curNativeRow: NativeRow = _

    def setWindow(data: Array[Array[Any]]): Unit = {
      javaData = data
      computer.resetWindow()
      if (curNativeRow != null) {
        curNativeRow.delete()
        curNativeRow = null
      }
      for (i <- data.indices.reverse) {
        val row = Row.fromSeq(data(i))
        val orderKey = computer.extractKey(row)
        computer.bufferRowOnly(row, orderKey)
        if (i == 0) {
          curRow = row
          curNativeRow = computer.getWindow.Get(0)
        }
      }
    }

    def run(): NativeRow = {
      CoreAPI.WindowProject(computer.getFn, computer.extractKey(curRow), curNativeRow, computer.getWindow)
    }
  }

  def recover(sqlConfig: OpenmldbBatchConfig, path: String, windowIdx: Int): SampleExecutor = {
    val fs = FileSystem.get(new Configuration())
    val configFile = fs.open(new Path(path + "/config.obj"))
    val configObjStream = new ObjectInputStream(configFile)
    val config = configObjStream.readObject().asInstanceOf[WindowAggConfig]

    val dataFile = fs.open(new Path(path + "/window_" + windowIdx + ".obj"))
    val objStream = new ObjectInputStream(dataFile)
    val data = objStream.readObject().asInstanceOf[Array[Array[Any]]]

    val executor = new SampleExecutor(sqlConfig, config)
    executor.setWindow(data)
    executor
  }
}

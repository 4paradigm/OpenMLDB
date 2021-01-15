package com._4paradigm.fesql.spark.nodes.window

import java.io._

import com._4paradigm.fesql.common.JITManager
import com._4paradigm.fesql.spark.nodes.WindowAggPlan.WindowAggConfig
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper}
import com._4paradigm.fesql.codec.{Row => NativeRow}
import com._4paradigm.fesql.spark.FeSQLConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory


class WindowSampleSupport(fs: FileSystem,
                          partitionIndex: Int,
                          config: WindowAggConfig,
                          jit: FeSQLJITWrapper) extends WindowHook {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val minWindowSize = config.sampleMinSize
  private val outputPath = config.sampleOutputPath + "/" + config.windowName + "/" + partitionIndex

  private var sampled = false

  override def postCompute(computer: WindowComputer, row: Row): Unit = {
    if (!sampled && computer.getWindow.size() >= minWindowSize) {
      sampled = true
      dumpModule()
      dumpConfig()
      dumpWindow(computer, s"$minWindowSize")
    }
  }

  def dumpModule(): Unit = {
    val buffer = config.moduleNoneBroadcast.getBuffer
    val bytes = new Array[Byte](buffer.capacity())
    buffer.get(bytes)
    dumpToFile(outputPath + "/module.ll", stream => stream.write(bytes))
  }

  def dumpConfig(): Unit = {
    val byteStream = new ByteArrayOutputStream()
    val ostream = new ObjectOutputStream(byteStream)
    ostream.writeObject(config)
    ostream.flush()
    ostream.close()
    dumpToFile(outputPath + "/config.obj",
      stream => byteStream.writeTo(stream))
  }

  def dumpWindow(computer: WindowComputer, desc: String): Unit = {
    val window = computer.getWindow
    val size = window.size().toInt
    val rows = (0 until size).map(idx => {
      val row = window.Get(idx)
      val fields = config.inputSchema.size
      val arr = new Array[Any](fields)
      computer.getEncoder.decode(row, arr)
      arr
    }).toArray

    val byteStream = new ByteArrayOutputStream()
    val ostream = new ObjectOutputStream(byteStream)
    ostream.writeObject(rows)
    ostream.flush()
    ostream.close()

    dumpToFile(outputPath + "/window_" + desc + ".obj",
      stream => byteStream.writeTo(stream))
  }

  def dumpToFile(path: String, write: OutputStream => Unit): Unit = {
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

  class SampleExecutor(sqlConfig: FeSQLConfig, val config: WindowAggConfig) {

    var javaData: Array[Array[Any]] = _

    private val jit = {
      val buffer = config.moduleNoneBroadcast.getBuffer
      JITManager.initJITModule(config.moduleTag, buffer)
      JITManager.getJIT(config.moduleTag)
    }

    private val computer = new WindowComputer(sqlConfig, config, jit)

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
        computer.bufferRowOnly(row)
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

  def recover(sqlConfig: FeSQLConfig, path: String): SampleExecutor = {
    val fs = FileSystem.get(new Configuration())
    val configFile = fs.open(new Path(path + "/config.obj"))
    val configObjStream = new ObjectInputStream(configFile)
    val config = configObjStream.readObject().asInstanceOf[WindowAggConfig]

    val windows = fs.listStatus(new Path(path + "/")).flatMap(status => {
      val path = status.getPath
      if (path.getName.startsWith("window")) {
        val dataFile = fs.open(path)
        val objStream = new ObjectInputStream(dataFile)
        val data = objStream.readObject().asInstanceOf[Array[Array[Any]]]
        Some(data)
      } else {
        None
      }
    })

    val executor = new SampleExecutor(sqlConfig, config)
    executor.setWindow(windows(0))
    executor
  }
}

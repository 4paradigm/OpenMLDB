package com._4paradigm.fesql.spark.nodes.window

import java.io._

import com._4paradigm.fesql.common.JITManager
import com._4paradigm.fesql.spark.nodes.WindowAggPlan.{WindowAggConfig, WindowComputer}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, WindowInterface}
import com._4paradigm.fesql.codec.{Row => NativeRow}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory


class WindowComputerWithSampleSupport(fs: FileSystem,
                                      config: WindowAggConfig,
                                      jit: FeSQLJITWrapper
                                     ) extends WindowComputer(config, jit) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val minWindowSize = config.sampleMinSize
  private val outputPath = config.sampleOutputPath + "/" + config.windowName

  private var sampled = false

  override def compute(row: Row): Row = {
    val output = super.compute(row)
    if (!sampled && window.size() >= minWindowSize) {
      sampled = true
      dumpModule()
      dumpConfig()
      dumpWindow(window, s"$minWindowSize")
    }
    output
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

  def dumpWindow(window: WindowInterface, desc: String): Unit = {
    val size = window.size().toInt
    val rows = (0 until size).map(idx => {
      val row = window.Get(idx)
      val fields = config.inputSchema.size
      val arr = new Array[Any](fields)
      encoder.decode(row, arr)
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


object WindowComputerWithSampleSupport {

  class SampleExecutor(val config: WindowAggConfig) {

    private val jit = {
      val buffer = config.moduleNoneBroadcast.getBuffer
      JITManager.initJITModule(config.moduleTag, buffer)
      JITManager.getJIT(config.moduleTag)
    }

    private val computer = new WindowComputer(config, jit)

    private var curRow: Row = _
    private var curNativeRow: NativeRow = _

    def setWindow(data: Array[Array[Any]]): Unit = {
      computer.resetWindow()
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

  def recover(path: String, windowName: String): SampleExecutor = {
    val fs = FileSystem.get(new Configuration())
    val configFile = fs.open(new Path(path + "/" + windowName + "/config.obj"))
    val configObjStream = new ObjectInputStream(configFile)
    val config = configObjStream.readObject().asInstanceOf[WindowAggConfig]

    val windows = fs.listStatus(new Path(path + "/" + windowName + "/")).flatMap(status => {
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

    val executor = new SampleExecutor(config)
    executor.setWindow(windows(0))
    executor
  }
}

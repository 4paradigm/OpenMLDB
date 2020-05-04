package com._4paradigm.fesql.offline.nodes

import java.nio.ByteBuffer

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.codec.{Row => NativeRow}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalWindowAggrerationNode, WindowInterface}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import sun.nio.ch.DirectBuffer

import scala.collection.mutable


object WindowAggPlan {

  def gen(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf(ctx.getSparkSession)
    val rdd = input.getRDD
    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())

    // process window op
    val windowOp = node.getWindow_();
    // process order key
    val orders = windowOp.getSort_().getOrders_().GetOrderBy()
    if (orders.GetChildNum() > 1) {
      throw new FeSQLException("Multiple window order not supported")
    }
    val orderName = SparkColumnUtil.resolveColName(orders.GetChild(0), inputDf, ctx)
    val orderIdx = inputDf.columns.indexOf(orderName)
    if (orderIdx < 0) {
      throw new FeSQLException(s"Fail to find column $orderName")
    }

    // process group-by keys
    val groups = windowOp.getGroup_.groups();
    val groupIdxs = mutable.ArrayBuffer[Int]()
    for (k <- 0 until groups.GetChildNum()) {
      val colName = SparkColumnUtil.resolveColName(groups.GetChild(k), inputDf, ctx)
      val colIdx = inputDf.columns.indexOf(colName)
      if (colIdx < 0) {
        throw new FeSQLException(s"Fail to find column $colName")
      }
      groupIdxs += colIdx
    }

    val windowAggConfig = WindowAggConfig(
      startOffset = node.getWindow_().getRange_.getStart_offset_(),
      orderIdx = orderIdx,
      groupIdxs = groupIdxs.toArray,
      functionName = node.getProject_().getFn_name_(),
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getModuleBufferBroadcast,
      inputSchema = input.getSchema,
      outputSchema = outputSchema
    )

    val resultRDD = rdd.mapPartitions(iter => {
      // ensure worker native
      val tag = windowAggConfig.moduleTag
      val buffer = windowAggConfig.moduleBroadcast.value.getBuffer
      JITManager.initJITModule(tag, buffer)
      val jit = JITManager.getJIT(tag)

      windowAggIter(iter, jit, windowAggConfig)
    })

    SparkInstance.fromRDD(outputSchema, resultRDD)
  }


  def windowAggIter(inputIter: Iterator[Row], jit: FeSQLJITWrapper, config: WindowAggConfig): Iterator[Row] = {
    var lastRow: Row = null
    var window = new WindowInterface(config.startOffset, 0, 0)

    // reusable output row inst
    val outputArr = Array.fill[Any](config.outputSchema.size)(null)

    val fn = jit.FindFunction(config.functionName)

    // TODO: these objects are now leaked
    val encoder = new SparkRowCodec(config.inputSchema)
    val decoder = new SparkRowCodec(config.outputSchema)

    // order key extractor
    val orderField = config.inputSchema(config.orderIdx)
    val orderKeyExtractor = createOrderKeyExtractor(
      config.orderIdx, orderField.dataType, orderField.nullable)

    // group key comparation
    val groupKeyComparator = createGroupKeyComparator(
      config.groupIdxs, config.inputSchema)

    // buffer management
    val bufferPool = mutable.ArrayBuffer[ByteBuffer]()

    inputIter.map(row => {
      // build new window for each group
      if (lastRow != null) {
        val groupChanged = groupKeyComparator.apply(row, lastRow)
        if (groupChanged) {
          // TODO: wrap iter to hook iter end; now last window is leak
          window.delete()
          window = new WindowInterface(config.startOffset, 0, 0)

          bufferPool.foreach(buf => {
            buf.asInstanceOf[DirectBuffer].cleaner().clean()
          })
          bufferPool.clear()
        }
      }
      lastRow = row

      // do window compute
      computeWindow(row, fn, bufferPool, orderKeyExtractor, window, encoder, decoder, outputArr)
    })
  }


  def computeWindow(row: Row,
                    fn: Long,
                    bufferPool: mutable.ArrayBuffer[ByteBuffer],
                    orderKeyExtractor: Row => Long,
                    window: WindowInterface,
                    encoder: SparkRowCodec,
                    decoder: SparkRowCodec,
                    outputArr: Array[Any]): Row = {
    // ensure direct buffer
    val rowSize = encoder.getNativeRowSize(row)

    // TODO: opt memory management
    val inputBuffer = ByteBuffer.allocateDirect(rowSize)
    bufferPool += inputBuffer

    // call encode
    encoder.encode(row, inputBuffer)

    // extract key
    val key = orderKeyExtractor.apply(row)

    // call native compute
    val nativeInputRow = new NativeRow(inputBuffer)
    val outputNativeRow = CoreAPI.WindowProject(fn, key, nativeInputRow, window)

    // call decode
    decoder.decode(outputNativeRow, outputArr)

    // release swig jni objects
    nativeInputRow.delete()
    outputNativeRow.delete()

    Row.fromSeq(outputArr)  // can reuse backed array
  }


  def createOrderKeyExtractor(keyIdx: Int, sparkType: DataType, nullable: Boolean): Row => Long = {
    sparkType match {
      case ShortType => row: Row => row.getShort(keyIdx).toLong
      case IntegerType => row: Row => row.getInt(keyIdx).toLong
      case LongType => row: Row => row.getLong(keyIdx)
      case TimestampType => row: Row => row.getTimestamp(keyIdx).getTime
      case _ =>
        throw new FeSQLException(s"Illegal window key type: $sparkType")
    }
  }


  def createGroupKeyComparator(keyIdxs: Array[Int], schema: StructType): (Row, Row) => Boolean = {
    if (keyIdxs.length == 1) {
       val idx = keyIdxs(0)
      (row1, row2) => {
        row1.get(idx) != row2.get(idx)
      }
    } else {
      (row1, row2) => {
        keyIdxs.exists(i => row1.get(i) != row2.get(i))
      }
    }
  }


  // spark closure class
  case class WindowAggConfig(startOffset: Long,
                             orderIdx: Int,
                             groupIdxs: Array[Int],
                             functionName: String,
                             moduleTag: String,
                             moduleBroadcast: Broadcast[SerializableByteBuffer],
                             inputSchema: StructType,
                             outputSchema: StructType)
}

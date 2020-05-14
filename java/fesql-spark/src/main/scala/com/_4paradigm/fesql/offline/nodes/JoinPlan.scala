package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.`type`.TypeOuterClass.ColumnDef
import com._4paradigm.fesql.codec.RowView
import com._4paradigm.fesql.node.JoinType
import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.offline.utils.{FesqlUtil, SparkColumnUtil, SparkRowUtil}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalJoinNode}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{Column, Row, functions}

import scala.collection.mutable


object JoinPlan {

  def gen(ctx: PlanContext, node: PhysicalJoinNode, left: SparkInstance, right: SparkInstance): SparkInstance = {
    val joinType = node.join().join_type()
    if (joinType != JoinType.kJoinTypeLeft && joinType != JoinType.kJoinTypeLast) {
      throw new FeSQLException(s"Join type $joinType not supported")
    }

    val sess = ctx.getSparkSession

    val indexName = "__JOIN_INDEX__-" + System.currentTimeMillis()
    val leftDf = {
      if (joinType == JoinType.kJoinTypeLast) {
        val indexedRDD = left.getRDD.zipWithIndex().map {
          case (row, id) => Row.fromSeq(row.toSeq :+ id)
        }
        sess.createDataFrame(indexedRDD,
          left.getSchema.add(indexName, LongType))

      } else {
        left.getDf(sess)
      }
    }

    val rightDf = right.getDf(sess)

    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)

    // get left index
    val leftKeys = node.join().left_key().keys()
    val leftKeyCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until leftKeys.GetChildNum()) {
      val expr = leftKeys.GetChild(i)
      val column = SparkColumnUtil.resolveLeftColumn(expr, node, leftDf, ctx)
      leftKeyCols += column
    }

    // get right index
    val rightKeys = node.join().right_key().keys()
    val rightKeyCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until rightKeys.GetChildNum()) {
      val expr = rightKeys.GetChild(i)
      val column = SparkColumnUtil.resolveRightColumn(expr, node, rightDf, ctx)
      rightKeyCols += column
    }

    // build join condition
    val joinConditions = mutable.ArrayBuffer[Column]()
    if (leftKeyCols.lengthCompare(rightKeyCols.length) != 0) {
      throw new FeSQLException("Illegal join key conditions")
    }

    // == key conditions
    for ((leftCol, rightCol) <- leftKeyCols.zip(rightKeyCols)) {
      // TODO: handle null value
      joinConditions += leftCol === rightCol
    }

    val filter = node.join().filter()
    // extra conditions
    if (filter.condition() != null) {

      val regName = "FESQL_JOIN_CONDITION_" + node.GetFnName()
      val conditionUDF = new JoinConditionUDF(
        functionName = filter.fn_info().fn_name(),
        inputSchemaSlices = inputSchemaSlices,
        outputSchema = filter.fn_info().fn_schema(),
        moduleTag = ctx.getTag,
        moduleBroadcast = ctx.getModuleBufferBroadcast
      )
      sess.udf.register(regName, conditionUDF)

      val allColWrap = functions.struct(
          leftDf.columns.filter(_ != indexName).map(leftDf.col) ++
          rightDf.columns.map(rightDf.col)
      : _*)
      joinConditions += functions.callUDF(regName, allColWrap)
    }

    if (joinConditions.isEmpty) {
      throw new FeSQLException("No join conditions specified")
    }

    val joined = leftDf.join(rightDf, joinConditions.reduce(_ && _),  "left")

    val result = if (joinType == JoinType.kJoinTypeLast) {
      val indexColIdx = leftDf.schema.size - 1

      val timeKey = node.join().right_key().keys().GetChild(0)
      val planLeftSize = node.GetProducer(0).GetOutputSchema().size()
      val timeColIdx = SparkColumnUtil.resolveColumnIndex(timeKey, node) - planLeftSize
      assert(timeColIdx >= 0)

      val timeIdxInJoined = timeColIdx + leftDf.schema.size
      val timeColType = rightDf.schema(timeColIdx).dataType

      import sess.implicits._

      val distinct = joined
        .groupByKey {
          row => row.getLong(indexColIdx)
        }
        .mapGroups {
          case (_, iter) =>
            val timeExtractor = SparkRowUtil.createOrderKeyExtractor(
              timeIdxInJoined, timeColType, nullable=false)

            iter.maxBy(row => timeExtractor.apply(row))

        }(RowEncoder(joined.schema))

      distinct.drop(indexName)

    } else {
      joined
    }

    SparkInstance.fromDataFrame(null, result)
  }


  class JoinConditionUDF(functionName: String,
                         inputSchemaSlices: Array[StructType],
                         outputSchema: java.util.List[ColumnDef],
                         moduleTag: String,
                         moduleBroadcast: Broadcast[SerializableByteBuffer]
                        ) extends Function1[Row, Boolean] with Serializable {

    @transient private lazy val tls = new ThreadLocal[UnSafeJoinConditionUDFImpl]() {
      override def initialValue(): UnSafeJoinConditionUDFImpl = {
        new UnSafeJoinConditionUDFImpl(
          functionName, inputSchemaSlices, outputSchema, moduleTag, moduleBroadcast)
      }
    }

    override def apply(row: Row): Boolean = {
      tls.get().apply(row)
    }
  }

  class UnSafeJoinConditionUDFImpl(functionName: String,
                                   inputSchemaSlices: Array[StructType],
                                   outputSchema: java.util.List[ColumnDef],
                                   moduleTag: String,
                                   moduleBroadcast: Broadcast[SerializableByteBuffer]
                                  ) extends Function1[Row, Boolean] with Serializable {
    private val jit = initJIT()

    private val fn: Long = jit.FindFunction(functionName)
    if (fn == 0) {
      throw new FeSQLException(s"Fail to find native jit function $functionName")
    }

    // TODO: these are leaked
    private val bufferPool: NativeBufferPool = new NativeBufferPool
    private val encoder: SparkRowCodec = new SparkRowCodec(inputSchemaSlices, bufferPool)
    private val outView: RowView = new RowView(outputSchema)

    def initJIT(): FeSQLJITWrapper = {
      // ensure worker native
      val buffer = moduleBroadcast.value.getBuffer
      JITManager.initJITModule(moduleTag, buffer)
      JITManager.getJIT(moduleTag)
    }

    override def apply(row: Row): Boolean = {
      // call encode
      val nativeInputRow = encoder.encode(row, keepBuffer=false)

      // call native compute
      val result = CoreAPI.ComputeCondition(fn, nativeInputRow, outView, 0)

      // release swig jni objects
      nativeInputRow.delete()

      result
    }
  }
}

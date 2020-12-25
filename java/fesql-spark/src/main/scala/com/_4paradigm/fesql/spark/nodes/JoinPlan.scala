package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.`type`.TypeOuterClass.ColumnDef
import com._4paradigm.fesql.codec.RowView
import com._4paradigm.fesql.common.{FesqlException, JITManager, SerializableByteBuffer}
import com._4paradigm.fesql.node.{ExprListNode, JoinType}
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.SparkColumnUtil.getColumnFromIndex
import com._4paradigm.fesql.spark.utils.{FesqlUtil, SparkColumnUtil, SparkRowUtil}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalJoinNode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{Column, Row, functions}

import scala.collection.mutable


object JoinPlan {

  def gen(ctx: PlanContext, node: PhysicalJoinNode, left: SparkInstance, right: SparkInstance): SparkInstance = {
    val joinType = node.join().join_type()
    if (joinType != JoinType.kJoinTypeLeft && joinType != JoinType.kJoinTypeLast) {
      throw new FesqlException(s"Join type $joinType not supported")
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

    val indexColIdx = if (joinType == JoinType.kJoinTypeLast) {
      leftDf.schema.size - 1
    } else {
      leftDf.schema.size
    }

    val rightDf = right.getDf(sess)

    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)

    // build join condition
    val joinConditions = mutable.ArrayBuffer[Column]()

    // Handle equal condiction
    if (node.join().left_key() != null && node.join().left_key().getKeys_ != null) {
      val leftKeys: ExprListNode = node.join().left_key().getKeys_
      val rightKeys: ExprListNode = node.join().right_key().getKeys_

      val keyNum = leftKeys.GetChildNum
      for (i <- 0 until keyNum) {
        val leftColumn = SparkColumnUtil.resolveExprNodeToColumn(leftKeys.GetChild(i), node.GetProducer(0), leftDf)
        val rightColumn = SparkColumnUtil.resolveExprNodeToColumn(rightKeys.GetChild(i), node.GetProducer(1), rightDf)
        joinConditions += (leftColumn === rightColumn)
      }
    }

    val filter = node.join().condition()
    // extra conditions
    if (filter.condition() != null) {
      val regName = "FESQL_JOIN_CONDITION_" + filter.fn_info().fn_name()
      val conditionUDF = new JoinConditionUDF(
        functionName = filter.fn_info().fn_name(),
        inputSchemaSlices = inputSchemaSlices,
        outputSchema = filter.fn_info().fn_schema(),
        moduleTag = ctx.getTag,
        moduleBroadcast = ctx.getSerializableModuleBuffer
      )
      sess.udf.register(regName, conditionUDF)

      // Handle the duplicated column names to get Spark Column by index
      val allColumns = new mutable.ArrayBuffer[Column]()
      for (i <- 0 until leftDf.schema.size) {
        if (i != indexColIdx) {
          allColumns += getColumnFromIndex(leftDf, i)
        }
      }
      for (i <- 0 until rightDf.schema.size) {
        allColumns += getColumnFromIndex(rightDf, i)
      }

      val allColWrap = functions.struct(allColumns:_*)
      joinConditions += functions.callUDF(regName, allColWrap)
    }

    if (joinConditions.isEmpty) {
      throw new FesqlException("No join conditions specified")
    }

    val joined = leftDf.join(rightDf, joinConditions.reduce(_ && _),  "left")

    val result = if (joinType == JoinType.kJoinTypeLast) {

      // TODO: Support multiple order by columns

      val hasOrderby = if (node.join.right_sort != null && node.join.right_sort.orders != null && node.join.right_sort.orders.order_by != null) {
        true
      } else {
        false
      }

      // Resolve order by column index
      if (hasOrderby) {
        val orderbyExprListNode = node.join.right_sort.orders.order_by
        val planLeftSize = node.GetProducer(0).GetOutputSchema().size()
        val timeColIdx = SparkColumnUtil.resolveColumnIndex(orderbyExprListNode.GetChild(0), node) - planLeftSize
        assert(timeColIdx >= 0)

        val timeIdxInJoined = timeColIdx + leftDf.schema.size
        val timeColType = rightDf.schema(timeColIdx).dataType

        val isAsc = node.join.right_sort.is_asc

        import sess.implicits._

        val distinct = joined
          .groupByKey {
            row => row.getLong(indexColIdx)
          }
          .mapGroups {
            case (_, iter) =>
              val timeExtractor = SparkRowUtil.createOrderKeyExtractor(
                timeIdxInJoined, timeColType, nullable=false)

              if (isAsc) {
                iter.maxBy(row => {
                  if (row.isNullAt(timeIdxInJoined)) {
                    Long.MinValue
                  } else {
                    timeExtractor.apply(row)
                  }
                })
              } else {
                iter.minBy(row => {
                  if (row.isNullAt(timeIdxInJoined)) {
                    Long.MaxValue
                  } else {
                    timeExtractor.apply(row)
                  }
                })
              }
          }(RowEncoder(joined.schema))

        distinct.drop(indexName)
      } else { // Does not have order by column
        // TODO: Do not use native last join currently
        joined.dropDuplicates(indexName).drop(indexName)
        /*
        try {
          org.apache.spark.sql.catalyst.plans.JoinType("last") // If Spark distribution support native last join type
          leftDf.join(rightDf, joinConditions.reduce(_ && _),  "last")
        } catch {
          case _: IllegalArgumentException => {
            // Randomly select the first row from joined table
            joined.dropDuplicates(indexName).drop(indexName)
          }
        }
        */
      }

    } else { // Just left join, not last join
      joined
    }

    SparkInstance.fromDataFrame(result)
  }


  class JoinConditionUDF(functionName: String,
                         inputSchemaSlices: Array[StructType],
                         outputSchema: java.util.List[ColumnDef],
                         moduleTag: String,
                         moduleBroadcast: SerializableByteBuffer
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
                                   moduleBroadcast: SerializableByteBuffer
                                  ) extends Function1[Row, Boolean] with Serializable {
    private val jit = initJIT()

    private val fn: Long = jit.FindFunction(functionName)
    if (fn == 0) {
      throw new FesqlException(s"Fail to find native jit function $functionName")
    }

    // TODO: these are leaked
    private val encoder: SparkRowCodec = new SparkRowCodec(inputSchemaSlices)
    private val outView: RowView = new RowView(outputSchema)

    def initJIT(): FeSQLJITWrapper = {
      // ensure worker native
      val buffer = moduleBroadcast.getBuffer
      JITManager.initJITModule(moduleTag, buffer)
      JITManager.getJIT(moduleTag)
    }

    override def apply(row: Row): Boolean = {
      // call encode
      val nativeInputRow = encoder.encode(row)

      // call native compute
      val result = CoreAPI.ComputeCondition(fn, nativeInputRow, outView, 0)

      // release swig jni objects
      nativeInputRow.delete()

      result
    }
  }
}

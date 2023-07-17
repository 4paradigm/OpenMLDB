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

package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.`type`.TypeOuterClass.ColumnDef
import com._4paradigm.hybridse.codec
import com._4paradigm.hybridse.codec.RowView
import com._4paradigm.hybridse.sdk.{HybridSeException, JitManager, SerializableByteBuffer, UnsupportedHybridSeException}
import com._4paradigm.hybridse.node.{BinaryExpr, ExprListNode, ExprType, FnOperator, JoinType}
import com._4paradigm.hybridse.vm.{CoreAPI, HybridSeJitWrapper, PhysicalJoinNode}
import com._4paradigm.openmldb.batch.utils.{ExpressionUtil, ExternalUdfUtil, HybridseUtil, SparkColumnUtil,
  SparkRowUtil, SparkUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance, SparkRowCodec}
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.slf4j.LoggerFactory

import scala.collection.mutable


object JoinPlan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalJoinNode, left: SparkInstance, right: SparkInstance): SparkInstance = {
    val joinType = node.join().join_type()
    if (joinType != JoinType.kJoinTypeLeft &&
      joinType != JoinType.kJoinTypeLast && joinType != JoinType.kJoinTypeConcat) {
      throw new HybridSeException(s"Join type $joinType not supported")
    }

    // Handle concat join
    if (joinType == JoinType.kJoinTypeConcat) {
      return ConcatJoinPlan.gen(ctx, node, left, right)
    }

    val spark = ctx.getSparkSession

    // TODO: Do not handle dataframe with index because ConcatJoin will not include LastJoin or LeftJoin node
    val rightDf = right.getDf()

    val isUnsafeRowOpt = ctx.getConf.enableUnsafeRowOptimization
    val inputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node, isUnsafeRowOpt)

    val hasOrderby =
      ((node.join.right_sort != null) && (node.join.right_sort.orders != null)
        && (node.join.right_sort.orders.getOrder_expressions_ != null)
        && (node.join.right_sort.orders.getOrder_expressions_.GetChildNum() != 0))

    // Check if we can use native last join
    val supportNativeLastJoin = SparkUtil.supportNativeLastJoin(joinType, hasOrderby)
    logger.info("Enable native last join or not: " + ctx.getConf.enableNativeLastJoin)

    val indexName = "__JOIN_INDEX__" + System.currentTimeMillis()

    var hasIndexColumn = false

    val leftDf: DataFrame = {
      if (joinType == JoinType.kJoinTypeLeft) {
        left.getDf()
      } else {
        if (supportNativeLastJoin && ctx.getConf.enableNativeLastJoin) {
          left.getDf()
        } else {
          hasIndexColumn = true
          // Add index column for original last join, not used in native last join
          SparkUtil.addIndexColumn(spark, left.getDf(), indexName, ctx.getConf.addIndexColumnMethod)
        }
      }
    }

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

    val indexColIdx = if (hasIndexColumn) {
      leftDf.schema.size - 1
    } else {
      -1
    }

    val filter = node.join().condition()
    // extra conditions
    if (filter.condition() != null) {
      if (ctx.getConf.enableJoinWithSparkExpr) {
        joinConditions += ExpressionUtil.recursiveGetSparkColumnFromExpr(filter.condition(), node, leftDf, rightDf,
          hasIndexColumn)
        logger.info("Generate spark join conditions: " + joinConditions)
      } else { // Disable join with native expression, use encoder/decoder and jit function
        val regName = "SPARKFE_JOIN_CONDITION_" + filter.fn_info().fn_name()
        var externalFunMap = Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun]()
        val openmldbSession = ctx.getOpenmldbSession
        if (ctx.getConf.openmldbZkCluster.nonEmpty && ctx.getConf.openmldbZkRootPath.nonEmpty
          && openmldbSession != null && openmldbSession.openmldbCatalogService != null) {
          externalFunMap = openmldbSession.openmldbCatalogService.getExternalFunctionsMap()
        }
        val conditionUDF = new JoinConditionUDF(
          functionName = filter.fn_info().fn_name(),
          inputSchemaSlices = inputSchemaSlices,
          outputSchema = filter.fn_info().fn_schema(),
          moduleTag = ctx.getTag,
          moduleBroadcast = ctx.getSerializableModuleBuffer,
          hybridseJsdkLibraryPath = ctx.getConf.openmldbJsdkLibraryPath,
          ctx.getConf.enableUnsafeRowOptimization,
          externalFunMap,
          ctx.getConf.taskmanagerExternalFunctionDir,
          ctx.getSparkSession.conf.get("spark.master").equalsIgnoreCase("yarn")
        )
        spark.udf.register(regName, conditionUDF)

        // Handle the duplicated column names to get Spark Column by index
        val allColumns = new mutable.ArrayBuffer[Column]()
        for (i <- leftDf.schema.indices) {
          if (i != indexColIdx) {
            allColumns += SparkColumnUtil.getColumnFromIndex(leftDf, i)
          }
        }
        for (i <- rightDf.schema.indices) {
          allColumns += SparkColumnUtil.getColumnFromIndex(rightDf, i)
        }

        val allColWrap = functions.struct(allColumns: _*)
        joinConditions += functions.callUDF(regName, allColWrap)
      }

    }

    if (joinConditions.isEmpty) {
      throw new HybridSeException("No join conditions specified")
    }

    val joined = leftDf.join(rightDf, joinConditions.reduce(_ && _), "left")

    val result = if (joinType == JoinType.kJoinTypeLeft) {
      joined
    } else if (joinType == JoinType.kJoinTypeLast) {
      // Resolve order by column index
      if (hasOrderby) {
        val orderExpr = node.join.right_sort.orders.GetOrderExpression(0)
        // Get the time column index from right table
        val timeColIdx = SparkColumnUtil.resolveOrderColumnIndex(orderExpr, node.GetProducer(1))
        assert(timeColIdx >= 0)
        val timeIdxInJoined = timeColIdx + leftDf.schema.size

        val isAsc = node.join.right_sort.is_asc

        val indexCol = SparkColumnUtil.getColumnFromIndex(joined, indexColIdx)

        val distinctRdd = joined.rdd.map({
          row => (row.getLong(indexColIdx), row)
        }).reduceByKey({
          (row1, row2) =>
            val timeColDataType = row1.schema.fields(timeIdxInJoined).dataType
            val rowTimeValue1 = SparkRowUtil.getLongFromIndex(timeIdxInJoined, timeColDataType, row1)
            val rowTimeValue2 = SparkRowUtil.getLongFromIndex(timeIdxInJoined, timeColDataType, row2)
            if (isAsc) {
              if (rowTimeValue1 > rowTimeValue2) {
                row1
              } else {
                row2
              }
            } else {
              if (rowTimeValue1 < rowTimeValue2) {
                row1
              } else {
                row2
              }
            }
        }).values

        val distinctDf = ctx.getSparkSession.createDataFrame(distinctRdd, joined.schema)
        distinctDf.drop(indexName)
      } else {
        if (supportNativeLastJoin && ctx.getConf.enableNativeLastJoin) {
          leftDf.join(rightDf, joinConditions.reduce(_ && _), "last")
        } else {
          joined.dropDuplicates(indexName).drop(indexName)
        }
      }
    } else {
      null
    }

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), result)
  }


  class JoinConditionUDF(functionName: String,
                         inputSchemaSlices: Array[StructType],
                         outputSchema: java.util.List[ColumnDef],
                         moduleTag: String,
                         moduleBroadcast: SerializableByteBuffer,
                         hybridseJsdkLibraryPath: String,
                         isUnsafeRowOpt: Boolean,
                         externalFunMap: Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun],
                         taskmanagerExternalFunctionDir: String,
                         isYarnMode: Boolean
                        ) extends Function1[Row, Boolean] with Serializable {

    @transient private lazy val tls = new ThreadLocal[UnSafeJoinConditionUDFImpl]() {
      override def initialValue(): UnSafeJoinConditionUDFImpl = {
        new UnSafeJoinConditionUDFImpl(
          functionName, inputSchemaSlices, outputSchema, moduleTag, moduleBroadcast, hybridseJsdkLibraryPath,
          isUnsafeRowOpt, externalFunMap, taskmanagerExternalFunctionDir, isYarnMode)
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
                                   moduleBroadcast: SerializableByteBuffer,
                                   openmldbJsdkLibraryPath: String,
                                   isUnafeRowOpt: Boolean,
                                   externalFunMap: Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun],
                                   taskmanagerExternalFunctionDir: String,
                                   isYarnMode: Boolean
                                  ) extends Function1[Row, Boolean] with Serializable {
    private val jit = initJIT()

    private val fn: Long = jit.FindFunction(functionName)
    if (fn == 0) {
      throw new HybridSeException(s"Fail to find native jit function $functionName")
    }

    // TODO: these are leaked
    private val encoder: SparkRowCodec = new SparkRowCodec(inputSchemaSlices)
    private val outView: RowView = new RowView(outputSchema)

    def initJIT(): HybridSeJitWrapper = {
      // before jit, more init
      SqlClusterExecutor.initJavaSdkLibrary(openmldbJsdkLibraryPath)

      // Load external udf if exists
      ExternalUdfUtil.executorRegisterExternalUdf(externalFunMap, taskmanagerExternalFunctionDir, isYarnMode)
      // ensure worker native
      val buffer = moduleBroadcast.getBuffer
      JitManager.initJitModule(moduleTag, buffer, isUnafeRowOpt)
      JitManager.getJit(moduleTag)
    }

    override def apply(row: Row): Boolean = {
      // call encode
      val nativeInputRow = encoder.encode(row)

      val emptyParameter = new codec.Row()

      // call native compute
      val result = CoreAPI.ComputeCondition(fn, nativeInputRow, emptyParameter, outView, 0)

      // release swig jni objects
      nativeInputRow.delete()

      result
    }
  }
}

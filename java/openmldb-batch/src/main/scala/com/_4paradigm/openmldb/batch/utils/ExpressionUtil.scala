package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.node.{ExprNode, ExprType}
import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException
import com._4paradigm.hybridse.vm.{CoreAPI, PhysicalOpNode}
import org.apache.spark.sql.{Column, DataFrame}

object ExpressionUtil {

  /**
   * Parse expr object to Spark Column object.
   * Notice that this only works for some non-computing expressions.
   *
   * @param expr
   * @param inputDf
   * @param physicalNode
   * @param parentNodeId
   * @return
   */
  def exprToSparkColumn(expr: ExprNode,
                        inputDf: DataFrame,
                        physicalNode: PhysicalOpNode,
                        parentNodeId: Int): Column = {
    expr.GetExprType() match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val inputNode = physicalNode.GetProducer(parentNodeId)
        val colIndex = CoreAPI.ResolveColumnIndex(inputNode, expr)
        if (colIndex < 0 || colIndex >= inputNode.GetOutputSchemaSize()) {
          inputNode.Print()
          inputNode.PrintSchema()
          throw new IndexOutOfBoundsException(
            s"${expr.GetExprString()} resolved index out of bound: $colIndex")
        }
        SparkColumnUtil.getColumnFromIndex(inputDf, colIndex)

      case _ => throw new UnsupportedHybridSeException(
        s"Simple project do not support expression type ${expr.GetExprType}")
    }
  }

/*
  def createSparkColumn(inputDf: DataFrame,
                        node: PhysicalSimpleProjectNode,
                        expr: ExprNode): (Column, HybridseDataType) = {
    expr.GetExprType() match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val inputNode = node.GetProducer(0)
        val colIndex = CoreAPI.ResolveColumnIndex(inputNode, expr)
        if (colIndex < 0 || colIndex >= inputNode.GetOutputSchemaSize()) {
          inputNode.Print()
          inputNode.PrintSchema()
          throw new IndexOutOfBoundsException(
            s"${expr.GetExprString()} resolved index out of bound: $colIndex")
        }
        val sparkCol = SparkColumnUtil.getColumnFromIndex(inputDf, colIndex)
        val sparkType = inputDf.schema(colIndex).dataType
        val schemaType = HybridseUtil.getHybridseType(sparkType)
        val innerType = HybridseUtil.getInnerTypeFromSchemaType(schemaType)
        sparkCol -> innerType

      case ExprType.kExprPrimary =>
        val const = ConstNode.CastFrom(expr)
        ConstProjectPlan.getConstCol(const) -> const.GetDataType

      case ExprType.kExprCast =>
        val cast = CastExprNode.CastFrom(expr)
        val castType = cast.getCast_type_
        val (childCol, childType) =
          createSparkColumn(inputDf, node, cast.GetChild(0))
        val castColumn = ConstProjectPlan.castSparkOutputCol(
          childCol, childType, castType)
        castColumn -> castType

      case _ => throw new UnsupportedHybridSeException(
        s"Simple project do not support expression type ${expr.GetExprType}")
    }


  }
  */


}

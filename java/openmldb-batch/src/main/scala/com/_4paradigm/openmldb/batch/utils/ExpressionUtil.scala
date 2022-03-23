package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.node.{BinaryExpr, ConstNode, ExprNode, ExprType}
import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException
import com._4paradigm.hybridse.vm.{CoreAPI, PhysicalOpNode}
import com._4paradigm.openmldb.batch.nodes.ConstProjectPlan
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.{Column, DataFrame}
import com._4paradigm.hybridse.node.{DataType => HybridseDataType}

object ExpressionUtil {

  /**
   * Convert const expression to Spark Column object.
   *
   * @param constNode
   * @return
   */
  def constExprToSparkColumn(constNode: ConstNode): Column = {
    constNode.GetDataType() match {
      case HybridseDataType.kNull => lit(null)

      case HybridseDataType.kInt16 =>
        typedLit[Short](constNode.GetAsInt16())

      case HybridseDataType.kInt32 =>
        typedLit[Int](constNode.GetAsInt32())

      case HybridseDataType.kInt64 =>
        typedLit[Long](constNode.GetAsInt64())

      case HybridseDataType.kFloat =>
        typedLit[Float](constNode.GetAsFloat())

      case HybridseDataType.kDouble =>
        typedLit[Double](constNode.GetAsDouble())

      case HybridseDataType.kVarchar =>
        typedLit[String](constNode.GetAsString())

      case _ => throw new UnsupportedHybridSeException(
        s"Const value for HybridSE type ${constNode.GetDataType()} not supported")
    }
  }

  /**
   * Convert expr object to Spark Column object.
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
      case ExprType.kExprPrimary =>
        val const = ConstNode.CastFrom(expr)
        ExpressionUtil.constExprToSparkColumn(const)
      case _ => throw new UnsupportedHybridSeException(
        s"Do not support converting expression to Spark Column for expression type ${expr.GetExprType}")
    }
  }

  /**
   * Convert binary expression to two Spark Column objects.
   *
   * @param binaryExpr
   * @param physicalNode
   * @param leftDf
   * @param rightDf
   * @return
   */
  def binaryExprToSparkColumns(binaryExpr: BinaryExpr, physicalNode: PhysicalOpNode, leftDf: DataFrame,
                               rightDf: DataFrame): (Column, Column) = {
    val leftExpr = binaryExpr.GetChild(0)
    val rightExpr = binaryExpr.GetChild(1)
    val leftSparkColumn = ExpressionUtil.exprToSparkColumn(leftExpr, leftDf, physicalNode, 0)
    val rightSparkColumn = ExpressionUtil.exprToSparkColumn(rightExpr, rightDf, physicalNode, 1)
    leftSparkColumn -> rightSparkColumn
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

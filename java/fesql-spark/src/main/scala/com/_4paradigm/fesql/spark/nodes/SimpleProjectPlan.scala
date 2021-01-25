package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.common.UnsupportedFesqlException
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.{FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.vm.{CoreAPI, PhysicalSimpleProjectNode}
import org.slf4j.LoggerFactory
import com._4paradigm.fesql.node.{CastExprNode, ConstNode, ExprNode, ExprType, DataType => FesqlDataType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._


object SimpleProjectPlan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalSimpleProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head
    val inputDf = inputInstance.getDf(ctx.getSparkSession)
    val outputSchema = node.GetOutputSchema()

    // Get the output column names from output schema
    val outputColNameList = outputSchema.asScala.map(col =>
      col.getName
    ).toList

    val outputColTypeList = outputSchema.asScala.map(col =>
      FesqlUtil.getInnerTypeFromSchemaType(col.getType)
    ).toList

    val selectColList = (0 until node.project().size.toInt).map(i => {
      val expr = node.project().GetExpr(i)
      val (col, innerType) = createSparkColumn(inputDf, node, expr)
      val castOutputCol = ConstProjectPlan.castSparkOutputCol(
        col, outputColTypeList(i), innerType)
      castOutputCol.alias(outputColNameList(i))
    }).toList

    // Use Spark DataFrame to select columns
    val result = SparkColumnUtil.setDataframeNullable(
      inputDf.select(selectColList: _*), nullable=true)
    SparkInstance.fromDataFrame(result)
  }

  /**
    * @param inputDf Input dataframe
    * @param node    Current plan node to explain project expression
    * @param expr    Simple project expression
    * @return   Spark column instance compatible with inner expression
    */
  def createSparkColumn(inputDf: DataFrame,
                        node: PhysicalSimpleProjectNode,
                        expr: ExprNode): (Column, FesqlDataType) = {
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
        val schemaType = FesqlUtil.getFeSQLType(sparkType)
        val innerType = FesqlUtil.getInnerTypeFromSchemaType(schemaType)
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

      case _ => throw new UnsupportedFesqlException(
        s"Simple project do not support expression type ${expr.GetExprType}")
    }
  }
}

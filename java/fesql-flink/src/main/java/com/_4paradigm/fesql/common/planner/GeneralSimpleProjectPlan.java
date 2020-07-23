package com._4paradigm.fesql.common.planner;

import com._4paradigm.fesql.common.FesqlException;
import com._4paradigm.fesql.common.FesqlUtil;
import com._4paradigm.fesql.node.ConstNode;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.ColumnSource;
import com._4paradigm.fesql.vm.PhysicalSimpleProjectNode;
import com._4paradigm.fesql.vm.SourceType;
import com._4paradigm.std.ColumnSourceList;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;

import java.util.ArrayList;
import java.util.List;

import static com._4paradigm.fesql.type.TypeOuterClass.Type.*;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;


public class GeneralSimpleProjectPlan {

    public static Table gen(GeneralPlanContext planContext, PhysicalSimpleProjectNode node, Table childTable) throws FesqlException {

        ColumnSourceList columnSourceList = node.getProject_().getColumn_sources_();

        // Get the output column names from output schema
        List<String> outputColNameList = new ArrayList<>();
        List<TypeOuterClass.Type> outputColTypeList = new ArrayList<>();
        for (TypeOuterClass.ColumnDef columnDef: node.GetOutputSchema()) {
            outputColNameList.add(columnDef.getName());
            outputColTypeList.add(columnDef.getType());
        }

        List<String> inputColNameList = new ArrayList<>();
        for (TypeOuterClass.ColumnDef columnDef: node.GetProducer(0).GetOutputSchema()) {
            inputColNameList.add(columnDef.getName());
        }


        List<ApiExpression> selectColumnList = new ArrayList<>();

        for (int i=0; i < columnSourceList.size(); ++i) {

            ColumnSource columnSource = columnSourceList.get(i);

            if (columnSource.type().swigValue() == SourceType.kSourceColumn.swigValue()) {
                int colIndex = FesqlUtil.resolveColumnIndex(columnSource.schema_idx(), columnSource.column_idx(), node.GetProducer(0));
                ApiExpression newCol = $(inputColNameList.get(colIndex)).as(outputColNameList.get(i));
                selectColumnList.add(newCol);

            } else if (columnSource.type().swigValue() == SourceType.kSourceConst.swigValue()) {
                ConstNode constNode = columnSource.const_value();
                String outputColName = outputColNameList.get(i);
                TypeOuterClass.Type columnType = outputColTypeList.get(i);

                if (columnType == kInt16) {
                    selectColumnList.add(lit(constNode.GetSmallInt()).cast(DataTypes.SMALLINT()).as(outputColName));
                } else if (columnType == kInt32) {
                    selectColumnList.add(lit(constNode.GetInt()).cast(DataTypes.INT()).as(outputColName));
                } else if (columnType == kInt64) {
                    selectColumnList.add(lit(constNode.GetLong()).cast(DataTypes.BIGINT()).as(outputColName));
                } else if (columnType== kFloat) {
                    selectColumnList.add(lit(constNode.GetFloat()).cast(DataTypes.FLOAT()).as(outputColName));
                } else if (columnType == kDouble) {
                    selectColumnList.add(lit(constNode.GetDouble()).cast(DataTypes.DOUBLE()).as(outputColName));
                } else if (columnType == kVarchar) {
                    selectColumnList.add(lit(constNode.GetStr()).cast(DataTypes.STRING()).as(outputColName));
                } else {
                    throw new FesqlException(String.format("FESQL type %s is not support as constant column", columnType));
                }

            }

        }


        // Java list to varargs
        return childTable.select(selectColumnList.toArray(new ApiExpression[0]));

    }

}

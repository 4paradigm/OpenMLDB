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

package com._4paradigm.hybridse.flink.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com._4paradigm.hybridse.common.FesqlException;
import com._4paradigm.hybridse.node.ColumnRefNode;
import com._4paradigm.hybridse.node.ExprNode;
import com._4paradigm.hybridse.node.ExprType;
import com._4paradigm.hybridse.vm.CoreAPI;
import com._4paradigm.hybridse.vm.PhysicalOpNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import com._4paradigm.hybridse.type.TypeOuterClass;
import com._4paradigm.hybridse.type.TypeOuterClass.Type;
import static com._4paradigm.hybridse.type.TypeOuterClass.Type.*;


public class FesqlUtil {

    /**
     * Build FESQL datatype with Flink datatype.
     */
    public static Type getFesqlType(DataType flinkDataType) throws FesqlException {
        LogicalType logicalType = flinkDataType.getLogicalType();
        if (logicalType instanceof IntType) {
            // Notice that no short or long in flink logical type
            return Type.kInt32;
        } else if (logicalType instanceof BigIntType) {
            return Type.kInt64;
        } else if (logicalType instanceof FloatType) {
            return Type.kFloat;
        } else if (logicalType instanceof DoubleType) {
            return Type.kDouble;
        } else if (logicalType instanceof VarCharType) {
            return Type.kVarchar;
        } else if (logicalType instanceof DateType) {
            return Type.kDate;
        } else if (logicalType instanceof TimestampType) {
            return Type.kTimestamp;
        } else {
            throw new FesqlException(String.format("Do not support Flink datatype %s", flinkDataType));
        }
    }

    /**
     * Build FESQL table def with Flink table schema.
     */
    public static TypeOuterClass.TableDef buildTableDef(String tableName, TableSchema tableSchema) throws FesqlException {

        TypeOuterClass.TableDef.Builder tableBuilder = TypeOuterClass.TableDef.newBuilder();
        tableBuilder.setName(tableName);

        for (TableColumn tableColumn: tableSchema.getTableColumns()) {
            TypeOuterClass.ColumnDef columnDef = TypeOuterClass.ColumnDef.newBuilder()
                    .setName(tableColumn.getName())
                    .setIsNotNull(!tableColumn.getType().getLogicalType().isNullable())
                    .setType(getFesqlType(tableColumn.getType()))
                    .build();
            tableBuilder.addColumns(columnDef);
        }

        return tableBuilder.build();
    }

    /**
     * Build FESQL database with map of table name and schema.
     */
    public static TypeOuterClass.Database buildDatabase(String dbName, Map<String, TableSchema> tableSchemaMap) throws FesqlException {

        TypeOuterClass.Database.Builder builder = TypeOuterClass.Database.newBuilder();
        builder.setName(dbName);

        for (Map.Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
            builder.addTables(buildTableDef(entry.getKey(), entry.getValue()));
        }

        return builder.build();
    }

    /**
     * Get the node output schema as list of slice and the slice is list of column def.
     */
    public static List<List<TypeOuterClass.ColumnDef>> getNodeOutputColumnLists(PhysicalOpNode node) {
        List<List<TypeOuterClass.ColumnDef>> outputLists = new ArrayList<List<TypeOuterClass.ColumnDef>>();

        for (int i=0; i < node.GetOutputSchemaSourceSize(); ++i) {
            List<TypeOuterClass.ColumnDef> columnDefs = node.GetOutputSchemaSource(i).GetSchema();
            outputLists.add(columnDefs);
        }

        return outputLists;
    }

    public static RowTypeInfo generateRowTypeInfo(List<TypeOuterClass.ColumnDef> columnDefs) throws FesqlException {
        int fieldNum = columnDefs.size();
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[fieldNum];

        for (int i=0; i < fieldNum; ++i) {
            TypeOuterClass.Type columnType = columnDefs.get(i).getType();

            if (columnType == kInt16) {
                fieldTypes[i] = Types.SHORT;
            } else if (columnType == kInt32) {
                fieldTypes[i] = Types.INT;
            } else if (columnType == kInt64) {
                // TODO: Catch TableException: Type is not supported: BigInteger
                fieldTypes[i] = Types.BIG_INT;
                //fieldTypes[i] = Types.INT;
            } else if (columnType== kFloat) {
                fieldTypes[i] = Types.FLOAT;
            } else if (columnType == kDouble) {
                fieldTypes[i] = Types.DOUBLE;
            } else if (columnType == kBool) {
                fieldTypes[i] = Types.BOOLEAN;
            } else if (columnType == kVarchar) {
                fieldTypes[i] = Types.STRING;
            } else if (columnType == kTimestamp) {
                // TODO: Make sure it is compatible with sql timestamp type
                fieldTypes[i] = Types.SQL_TIMESTAMP;
            } else if (columnType == kDate) {
                fieldTypes[i] = Types.SQL_DATE;
            } else {
                throw new FesqlException(String.format("Fail to convert row type info with %s", columnType));
            }

        }

        return new RowTypeInfo(fieldTypes);
    }

    public static int resolveColumnIndex(ExprNode exprNode, PhysicalOpNode physicalOpNode) throws FesqlException {
        if (exprNode.getExpr_type_().swigValue() == ExprType.kExprColumnRef.swigValue()) {
            int index = CoreAPI.ResolveColumnIndex(physicalOpNode, ColumnRefNode.CastFrom(exprNode));
            if (index < 0) {
                throw new FesqlException("Fail to resolve column ref expression node and get index " + index);
            } else if (index >= physicalOpNode.GetOutputSchema().size()) {
                throw new FesqlException("Fail to resolve column ref  expression node and get index " + index);
            } else {
                return index;
            }
        } else {
            throw new FesqlException("Do not support nono-columnref expression");
        }
    }
}

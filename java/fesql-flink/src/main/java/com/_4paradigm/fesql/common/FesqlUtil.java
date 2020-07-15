package com._4paradigm.fesql.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com._4paradigm.fesql.batch.FeSQLException;
import com._4paradigm.fesql.vm.PhysicalOpNode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.type.TypeOuterClass.Type;
import org.apache.flink.types.Row;

public class FesqlUtil {

    /**
     * Build FESQL datatype with Flink datatype.
     */
    public static Type getFesqlType(DataType flinkDataType) throws FeSQLException {
        LogicalType logicalType = flinkDataType.getLogicalType();
        if (logicalType instanceof IntType) {
            // Notice that no short or long in flink logical type
            return Type.kInt32;
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
            throw new FeSQLException(String.format("Do not support Flink datatype %s", flinkDataType));
        }
    }

    /**
     * Build FESQL table def with Flink table schema.
     */
    public static TypeOuterClass.TableDef buildTableDef(String tableName, TableSchema tableSchema) throws FeSQLException {

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
    public static TypeOuterClass.Database buildDatabase(String dbName, Map<String, TableSchema> tableSchemaMap) throws FeSQLException {

        TypeOuterClass.Database.Builder builder = TypeOuterClass.Database.newBuilder();
        builder.setName(dbName);

        for (Map.Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());

            builder.addTables(buildTableDef(entry.getKey(), entry.getValue()));
        }

        return builder.build();
    }

    /**
     * Get the node output schema as list of slice and the slice is list of column def.
     */
    public static List<List<TypeOuterClass.ColumnDef>> getNodeOutputColumnLists(PhysicalOpNode node) {
        List<List<TypeOuterClass.ColumnDef>> outputLists = new ArrayList<List<TypeOuterClass.ColumnDef>>();

        for (int i=0; i < node.GetOutputSchemaListSize(); ++i) {
            List<TypeOuterClass.ColumnDef> columnDefs = node.GetOutputSchemaSlice(i);
            outputLists.add(columnDefs);
        }

        return outputLists;
    }

    /**
     * Get the merged node output schema as list of column def.
     */
    public static List<TypeOuterClass.ColumnDef> getMergedNodeOutputColumnList(PhysicalOpNode node) {
        List<TypeOuterClass.ColumnDef> outputList = new ArrayList<TypeOuterClass.ColumnDef>();

        for (int i=0; i < node.GetOutputSchemaListSize(); ++i) {
            List<TypeOuterClass.ColumnDef> columnDefs = node.GetOutputSchemaSlice(i);
            outputList.addAll(columnDefs);
        }

        return outputList;
    }


}

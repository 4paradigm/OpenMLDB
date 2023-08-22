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

package com._4paradigm.openmldb.sdk;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.proto.Type;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class Common {
    public static int type2SqlType(DataType dataType) throws SQLException {
        if (dataType == DataType.kTypeBool) {
            return Types.BOOLEAN;
        } else if (dataType == DataType.kTypeInt16) {
            return Types.SMALLINT;
        } else if (dataType == DataType.kTypeInt32) {
            return Types.INTEGER;
        } else if (dataType == DataType.kTypeInt64) {
            return Types.BIGINT;
        } else if (dataType == DataType.kTypeFloat) {
            return Types.FLOAT;
        } else if (dataType == DataType.kTypeDouble) {
            return Types.DOUBLE;
        } else if (dataType == DataType.kTypeString) {
            return Types.VARCHAR;
        } else if (dataType == DataType.kTypeDate) {
            return Types.DATE;
        } else if (dataType == DataType.kTypeTimestamp) {
            return Types.TIMESTAMP;
        } else {
            throw new SQLException("Unexpected value: " + dataType.toString());
        }
    }

    public static com._4paradigm.openmldb.proto.Type.DataType sqlType2ProtoType(int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.BOOLEAN:
                return Type.DataType.kBool;
            case Types.SMALLINT:
                return Type.DataType.kSmallInt;
            case Types.INTEGER:
                return Type.DataType.kInt;
            case Types.BIGINT:
                return Type.DataType.kBigInt;
            case Types.FLOAT:
                return Type.DataType.kFloat;
            case Types.DOUBLE:
                return Type.DataType.kDouble;
            case Types.VARCHAR:
                return Type.DataType.kString;
            case Types.TIMESTAMP:
                return Type.DataType.kTimestamp;
            case Types.DATE:
                return Type.DataType.kDate;
            default:
                throw new SQLException("Unexpected value: " + sqlType);
        }
    }

    public static com._4paradigm.openmldb.sdk.Schema convertSchema(com._4paradigm.openmldb.Schema schema) throws SQLException {
        if (schema == null || schema.GetColumnCnt() == 0) {
            throw new SQLException("schema is null or empty");
        }
        List<Column> columnList = new ArrayList<>();
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            Column column = new Column();
            column.setColumnName(schema.GetColumnName(i));
            column.setSqlType(type2SqlType(schema.GetColumnType(i)));
            column.setNotNull(schema.IsColumnNotNull(i));
            column.setConstant(schema.IsConstant(i));
            columnList.add(column);
        }
        return new com._4paradigm.openmldb.sdk.Schema(columnList);
    }

    public static List<com._4paradigm.openmldb.proto.Common.ColumnDesc> convert2ProtoSchema(com._4paradigm.openmldb.sdk.Schema schema) throws SQLException {
        List<com._4paradigm.openmldb.proto.Common.ColumnDesc> columnList = new ArrayList<>();
        for (Column column : schema.getColumnList()) {
            com._4paradigm.openmldb.proto.Common.ColumnDesc.Builder builder = com._4paradigm.openmldb.proto.Common.ColumnDesc.newBuilder();
            builder.setName(column.getColumnName())
                    .setDataType(sqlType2ProtoType(column.getSqlType()))
                    .setNotNull(column.isNotNull());
            columnList.add(builder.build());
        }
        return columnList;
    }

    public static DataType sqlTypeToDataType(int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.BOOLEAN:
                return DataType.kTypeBool;
            case Types.SMALLINT:
                return DataType.kTypeInt16;
            case Types.INTEGER:
                return DataType.kTypeInt32;
            case Types.BIGINT:
                return DataType.kTypeInt64;
            case Types.FLOAT:
                return DataType.kTypeFloat;
            case Types.DOUBLE:
                return DataType.kTypeDouble;
            case Types.VARCHAR:
                return DataType.kTypeString;
            case Types.DATE:
                return DataType.kTypeDate;
            case Types.TIMESTAMP:
                return DataType.kTypeTimestamp;
            default:
                throw new SQLException("Unexpected Values: " + sqlType);
        }
    }

    public static ProcedureInfo convertProcedureInfo(com._4paradigm.openmldb.ProcedureInfo procedureInfo) throws SQLException {
        ProcedureInfo spInfo = new ProcedureInfo();
        spInfo.setDbName(procedureInfo.GetDbName());
        spInfo.setProName(procedureInfo.GetSpName());
        spInfo.setSql(procedureInfo.GetSql());
        spInfo.setInputSchema(Common.convertSchema(procedureInfo.GetInputSchema()));
        spInfo.setOutputSchema(Common.convertSchema(procedureInfo.GetOutputSchema()));
        spInfo.setMainTable(procedureInfo.GetMainTable());
        spInfo.setInputTables(procedureInfo.GetTables());
        spInfo.setInputDbs(procedureInfo.GetDbs());
        spInfo.setRouterCol(procedureInfo.GetRouterCol());
        return spInfo;
    }
}

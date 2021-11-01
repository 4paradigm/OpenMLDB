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

    public static DataType toDataType(int sqlType) throws SQLException {
        if (Types.BOOLEAN == sqlType) {
            return DataType.kTypeBool;
        } else if (Types.SMALLINT == sqlType) {
            return DataType.kTypeInt16;
        } else if (Types.INTEGER == sqlType) {
            return DataType.kTypeInt32;
        } else if (Types.BIGINT == sqlType) {
          return DataType.kTypeInt64;
        } else if (Types.FLOAT == sqlType) {
            return DataType.kTypeFloat;
        } else if (Types.DOUBLE == sqlType) {
            return DataType.kTypeDouble;
        } else if (Types.VARCHAR == sqlType) {
            return DataType.kTypeString;
        } else if (Types.DATE == sqlType) {
            return DataType.kTypeDate;
        } else if (Types.TIMESTAMP == sqlType) {
            return DataType.kTypeTimestamp;
        } else {
            throw new SQLException("Unexpected Values: " + sqlType);
        }
    }

    public static com._4paradigm.openmldb.sdk.Schema convertSchema(
            com._4paradigm.openmldb.Schema schema) throws SQLException {
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
}

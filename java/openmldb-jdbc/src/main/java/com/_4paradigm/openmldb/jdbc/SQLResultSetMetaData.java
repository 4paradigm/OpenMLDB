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

package com._4paradigm.openmldb.jdbc;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class SQLResultSetMetaData implements ResultSetMetaData {

    private Schema schema;

    public SQLResultSetMetaData(Schema schema) {
        this.schema = schema;
    }

    private void checkSchemaNull() throws SQLException {
        if (schema == null) {
            throw new SQLException("schema is null");
        }
    }

    private void checkIdx(int i) throws SQLException {
        if (i <= 0) {
            throw new SQLException("index underflow");
        }
        if (i > schema.GetColumnCnt()) {
            throw new SQLException("index overflow");
        }
    }

    private int type2SqlType(DataType dataType) throws SQLException{
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

    public void check(int i) throws SQLException {
        checkIdx(i);
        checkSchemaNull();
    }

    @Override
    public int getColumnCount() throws SQLException {
        checkSchemaNull();
        return schema.GetColumnCnt();
    }

    @Override
    @Deprecated
    public boolean isAutoIncrement(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isCaseSensitive(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isSearchable(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isCurrency(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public int isNullable(int i) throws SQLException {
        check(i);
        if (schema.IsColumnNotNull(i - 1)) {
            return columnNoNulls;
        } else {
            return columnNullable;
        }
    }

    @Override
    @Deprecated
    public boolean isSigned(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getColumnDisplaySize(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public String getColumnLabel(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public String getColumnName(int i) throws SQLException {
        check(i);
        return schema.GetColumnName(i - 1);
    }

    @Override
    @Deprecated
    public String getSchemaName(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getPrecision(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getScale(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public String getTableName(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public String getCatalogName(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        check(i);
        DataType dataType = schema.GetColumnType(i - 1);
        return type2SqlType(dataType);
    }

    @Override
    @Deprecated
    public String getColumnTypeName(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isReadOnly(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isWritable(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isDefinitelyWritable(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public String getColumnClassName(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        throw new SQLException("current do not support this method");
    }
}

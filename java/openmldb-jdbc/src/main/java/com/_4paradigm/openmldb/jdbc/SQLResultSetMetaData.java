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

import com._4paradigm.openmldb.sdk.Schema;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SQLResultSetMetaData implements ResultSetMetaData {

    private final Schema schema;

    public SQLResultSetMetaData(Schema schema) {
        this.schema = schema;
    }

    private void checkIdx(int i) throws SQLException {
        if (i <= 0) {
            throw new SQLException("index underflow");
        }
        if (i > schema.getColumnList().size()) {
            throw new SQLException("index overflow");
        }
    }

    public void check(int i) throws SQLException {
        checkIdx(i);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return schema.getColumnList().size();
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
        if (schema.getColumnList().get(i - 1).isNotNull()) {
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
        return schema.getColumnList().get(i - 1).getColumnName();
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
        return schema.getColumnList().get(i - 1).getSqlType();
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

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

package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.jdbc.OpenMLDBResultSet;
import com._4paradigm.openmldb.sdk.Common;
import java.sql.*;

public class SQLResultSet extends OpenMLDBResultSet {
    private com._4paradigm.openmldb.ResultSet resultSet;

    public SQLResultSet(com._4paradigm.openmldb.ResultSet resultSet) {
        this.resultSet = resultSet;
        if (resultSet != null) {
            totalRows = resultSet.Size();
            try {
                this.schema = Common.convertSchema(resultSet.GetSchema());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void check(int i, int type) throws SQLException {
        checkIdx(i);
        checkDataType(i, type);
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("resultset closed");
        }
    }

    private void checkIdx(int i) throws SQLException {
        if (i <= 0) {
            throw new SQLException("index underflow");
        }
        if (i > schema.size()) {
            throw new SQLException("index overflow");
        }
    }

    private void checkResultSetNull() throws SQLException{
        if (this.resultSet == null) {
            throw new SQLException("resultset is null");
        }
    }

    private void checkDataType(int i, int type) throws SQLException {
        if (schema.getColumnType(i - 1) != type) {
            throw new SQLException(String.format("data type not match, get %d and expect %d",
                    schema.getColumnType(i - 1), type));
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        checkResultSetNull();
        if (this.resultSet.Next()) {
            this.rowNum++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        if (resultSet != null) {
            resultSet.delete();
            resultSet = null;
        }
        closed = true;
    }

    @Override
    public String getString(int i) throws SQLException {
        check(i, Types.VARCHAR);
        if (this.resultSet.IsNULL(i - 1)) {
            return null;
        }
        return this.resultSet.GetStringUnsafe(i - 1);
    }

    @Override
    public boolean getBoolean(int i) throws SQLException {
        check(i, Types.BOOLEAN);
        if (this.resultSet.IsNULL(i - 1)) {
            return false;
        }
        return this.resultSet.GetBoolUnsafe(i - 1);
    }

    @Override
    public short getShort(int i) throws SQLException {
        check(i, Types.SMALLINT);
        if (this.resultSet.IsNULL(i - 1)) {
            return 0;
        }
        return this.resultSet.GetInt16Unsafe(i - 1);
    }

    @Override
    public int getInt(int i) throws SQLException {
        check(i, Types.INTEGER);
        if (this.resultSet.IsNULL(i - 1)) {
            return 0;
        }
        return resultSet.GetInt32Unsafe(i - 1);
    }

    @Override
    public long getLong(int i) throws SQLException {
        check(i, Types.BIGINT);
        if (this.resultSet.IsNULL(i - 1)) {
            return 0;
        }
        return this.resultSet.GetInt64Unsafe(i - 1);
    }

    @Override
    public float getFloat(int i) throws SQLException {
        check(i, Types.FLOAT);
        if (this.resultSet.IsNULL(i - 1)) {
            return 0.0f;
        }
        return this.resultSet.GetFloatUnsafe(i - 1);
    }

    @Override
    public double getDouble(int i) throws SQLException {
        check(i, Types.DOUBLE);
        if (this.resultSet.IsNULL(i - 1)) {
            return 0.0;
        }
        return resultSet.GetDoubleUnsafe(i - 1);
    }

    @Override
    public Date getDate(int i) throws SQLException {
        check(i, Types.DATE);
        if (this.resultSet.IsNULL(i - 1)) {
            return null;
        }
        com._4paradigm.openmldb.Date date = this.resultSet.GetStructDateUnsafe(i - 1);
        return new Date(date.getYear() - 1900, date.getMonth() - 1, date.getDay());
    }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        check(i, Types.TIMESTAMP);
        if (this.resultSet.IsNULL(i - 1)) {
            return null;
        }
        return new Timestamp(this.resultSet.GetTimeUnsafe(i -1));
    }

    @Override
    public String getNString(int i) throws SQLException {
        checkClosed();
        checkResultSetNull();
        checkIdx(i);
        if (this.resultSet.IsNULL(i - 1)) {
            return null;
        }
        return this.resultSet.GetAsStringUnsafe(i - 1);
    }
}

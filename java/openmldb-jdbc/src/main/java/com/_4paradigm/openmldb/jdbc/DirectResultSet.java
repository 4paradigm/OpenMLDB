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

import com._4paradigm.openmldb.common.codec.RowView;
import com._4paradigm.openmldb.sdk.Schema;

import java.nio.ByteBuffer;
import java.sql.*;

public abstract class DirectResultSet extends SQLResultSet {

    protected ByteBuffer buf;
    protected RowView rowView;

    public DirectResultSet(ByteBuffer buf, int totalRows, Schema schema) {
        this.buf = buf;
        this.totalRows = totalRows;
        this.schema = schema;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public String getString(int i) throws SQLException {
        int realIdx = i - 1;
        try {
            return rowView.getString(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public boolean getBoolean(int i) throws SQLException {
        int realIdx = i - 1;
        if (rowView.isNull(realIdx)) {
            return false;
        }
        try {
            return rowView.getBool(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public short getShort(int i) throws SQLException {
        int realIdx = i - 1;
        if (rowView.isNull(realIdx)) {
            return 0;
        }
        try {
            return rowView.getSmallInt(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public int getInt(int i) throws SQLException {
        int realIdx = i - 1;
        if (rowView.isNull(realIdx)) {
            return 0;
        }
        try {
            return rowView.getInt(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public long getLong(int i) throws SQLException {
        int realIdx = i - 1;
        if (rowView.isNull(realIdx)) {
            return 0;
        }
        try {
            return rowView.getBigInt(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public float getFloat(int i) throws SQLException {
        int realIdx = i - 1;
        if (rowView.isNull(realIdx)) {
            return 0.0f;
        }
        try {
            return rowView.getFloat(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public double getDouble(int i) throws SQLException {
        int realIdx = i - 1;
        if (rowView.isNull(realIdx)) {
            return 0.0;
        }
        try {
            return rowView.getDouble(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public Date getDate(int i) throws SQLException {
        int realIdx = i - 1;
        try {
            return rowView.getDate(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        int realIdx = i - 1;
        try {
            return rowView.getTimestamp(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public String getNString(int i) throws SQLException {
        int realIdx = i - 1;
        try {
            if (rowView.isNull(realIdx)) {
                return null;
            }
            return rowView.getString(realIdx);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }
}

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

import com._4paradigm.openmldb.QueryFuture;
import com._4paradigm.openmldb.sdk.Common;
import com._4paradigm.openmldb.sdk.Schema;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class SQLResultSet implements ResultSet {
    private com._4paradigm.openmldb.ResultSet resultSet;
    private boolean closed = false;
    private int rowNum = 0;
    private QueryFuture queryFuture;
    private Schema schema;

    public SQLResultSet(com._4paradigm.openmldb.ResultSet resultSet) {
        this.resultSet = resultSet;
        if (resultSet != null) {
            try {
                this.schema = Common.convertSchema(resultSet.GetSchema());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public SQLResultSet(com._4paradigm.openmldb.ResultSet resultSet, Schema schema) {
        this.resultSet = resultSet;
        this.schema = schema;
    }

    public SQLResultSet(com._4paradigm.openmldb.ResultSet resultSet, QueryFuture future, Schema schema) {
        this.resultSet = resultSet;
        this.queryFuture = future;
        this.schema = schema;
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

    public Schema GetInternalSchema() {
        return schema;
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
        this.resultSet.delete();
        this.resultSet = null;
        if (queryFuture != null) {
            queryFuture.delete();
            queryFuture = null;
        }
        this.closed = true;
    }

    @Override
    @Deprecated
    public boolean wasNull() throws SQLException {
        throw new SQLException("current do not support this method");
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
    @Deprecated
    public byte getByte(int i) throws SQLException {
        throw new SQLException("current do not support this method");
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
    @Deprecated
    public BigDecimal getBigDecimal(int i, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public byte[] getBytes(int i) throws SQLException {
        throw new SQLException("current do not support this method");
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
    @Deprecated
    public Time getTime(int i) throws SQLException {
        throw new SQLException("current do not support this method");
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
    @Deprecated
    public InputStream getAsciiStream(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public InputStream getBinaryStream(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public String getString(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean getBoolean(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public byte getByte(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public short getShort(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getInt(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public long getLong(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public float getFloat(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public double getDouble(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String s, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public byte[] getBytes(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Date getDate(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Time getTime(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Timestamp getTimestamp(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public InputStream getAsciiStream(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    /**
     * @param s the string for the unicode stream extraction.
     * @deprecated
     */
    @Override
    @Deprecated
    public InputStream getUnicodeStream(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public InputStream getBinaryStream(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void clearWarnings() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public String getCursorName() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public SQLResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        checkResultSetNull();
        return new SQLResultSetMetaData(schema);
    }

    @Override
    @Deprecated
    public Object getObject(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Object getObject(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int findColumn(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Reader getCharacterStream(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Reader getCharacterStream(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isAfterLast() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isFirst() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isLast() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void beforeFirst() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void afterLast() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean first() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean last() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        checkResultSetNull();
        return this.rowNum;
    }

    @Override
    @Deprecated
    public boolean absolute(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean relative(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean previous() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setFetchDirection(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getFetchDirection() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setFetchSize(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getFetchSize() throws SQLException {
        checkClosed();
        checkResultSetNull();
        return this.resultSet.Size();
    }

    @Override
    @Deprecated
    public int getType() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getConcurrency() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean rowUpdated() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean rowInserted() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean rowDeleted() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNull(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBoolean(int i, boolean b) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateByte(int i, byte b) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateShort(int i, short i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateInt(int i, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateLong(int i, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateFloat(int i, float v) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateDouble(int i, double v) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateString(int i, String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBytes(int i, byte[] bytes) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateDate(int i, Date date) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateTime(int i, Time time) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateObject(int i, Object o, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateObject(int i, Object o) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNull(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBoolean(String s, boolean b) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateByte(String s, byte b) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateShort(String s, short i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateInt(String s, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateLong(String s, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateFloat(String s, float v) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateDouble(String s, double v) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateString(String s, String s1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBytes(String s, byte[] bytes) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateDate(String s, Date date) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateTime(String s, Time time) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateObject(String s, Object o, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateObject(String s, Object o) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void insertRow() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateRow() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void deleteRow() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void refreshRow() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void cancelRowUpdates() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void moveToInsertRow() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void moveToCurrentRow() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Statement getStatement() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Ref getRef(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Blob getBlob(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Clob getClob(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Array getArray(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Ref getRef(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Blob getBlob(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Clob getClob(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Array getArray(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Date getDate(int i, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Date getDate(String s, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Time getTime(int i, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Time getTime(String s, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public URL getURL(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public URL getURL(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateRef(int i, Ref ref) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateRef(String s, Ref ref) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBlob(int i, Blob blob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBlob(String s, Blob blob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateClob(int i, Clob clob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateClob(String s, Clob clob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateArray(int i, Array array) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateArray(String s, Array array) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public RowId getRowId(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public RowId getRowId(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateRowId(int i, RowId rowId) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateRowId(String s, RowId rowId) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getHoldability() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    @Deprecated
    public void updateNString(int i, String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNString(String s, String s1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNClob(int i, NClob nClob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNClob(String s, NClob nClob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public NClob getNClob(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public NClob getNClob(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public SQLXML getSQLXML(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public SQLXML getSQLXML(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
        throw new SQLException("current do not support this method");
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

    @Override
    @Deprecated
    public String getNString(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Reader getNCharacterStream(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Reader getNCharacterStream(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateClob(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateClob(String s, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNClob(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNClob(String s, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNCharacterStream(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNCharacterStream(String s, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateCharacterStream(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateCharacterStream(String s, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBlob(int i, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateBlob(String s, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateClob(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateClob(String s, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNClob(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void updateNClob(String s, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public <T> T getObject(int i, Class<T> aClass) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public <T> T getObject(String s, Class<T> aClass) throws SQLException {
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

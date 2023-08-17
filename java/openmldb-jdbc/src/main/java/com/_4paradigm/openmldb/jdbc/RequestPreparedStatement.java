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

import com._4paradigm.openmldb.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestPreparedStatement implements java.sql.PreparedStatement {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    protected String db;
    protected String currentSql;
    protected SQLRouter router;
    protected SQLRequestRow currentRow;
    protected Schema currentSchema;
    protected List<Object> currentDatas;
    protected List<Boolean> hasSet;
    protected boolean closed = false;
    protected boolean closeOnComplete = false;
    protected Map<Integer, Integer> stringsLen = new HashMap<>();

    private void checkNull() throws SQLException {
        if (db == null) {
            throw new SQLException("db is null");
        }
        if (router == null) {
            throw new SQLException("SQLRouter is null");
        }
        if (currentRow == null) {
            throw new SQLException("SQLRequestRow is null");
        }
        if (currentSchema == null) {
            throw new SQLException("schema is null");
        }
        if (currentDatas == null) {
            throw new SQLException("currentDatas is null");
        }
        if (hasSet == null) {
            throw new SQLException("hasSet is null");
        }
    }

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("preparedstatement closed");
        }
    }

    protected void checkExecutorClosed() throws SQLException {
        if (router == null) {
            throw new SQLException("Executor close");
        }
    }

    void checkIdx(int i) throws SQLException {
        checkClosed();
        checkNull();
        if (i <= 0) {
            throw new SQLException("index underflow, index: " + i + " size: " + this.currentSchema.GetColumnCnt());
        }
        if (i > this.currentSchema.GetColumnCnt()) {
            throw new SQLException("index overflow, index: " + i + " size: " + this.currentSchema.GetColumnCnt());
        }
    }

    private void checkType(int i, DataType type) throws SQLException {
        if (this.currentSchema.GetColumnType(i - 1) != type) {
            throw new SQLException("data type not match");
        }
    }

    @Override
    public SQLResultSet executeQuery() throws SQLException {
        checkClosed();
        checkExecutorClosed();
        dataBuild();
        Status status = new Status();
        com._4paradigm.openmldb.ResultSet resultSet = router.ExecuteSQLRequest(db, currentSql, currentRow, status);
        if (resultSet == null || status.getCode() != 0) {
            String msg = status.ToString();
            status.delete();
            if (resultSet != null) {
                resultSet.delete();
            }
            throw new SQLException("execute sql fail, msg: " + msg);
        }
        status.delete();
        SQLResultSet rs = new SQLResultSet(resultSet);
        if (closeOnComplete) {
            closed = true;
        }
        return rs;
    }

    @Override
    @Deprecated
    public int executeUpdate() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    private void setNull(int i) throws SQLException {
        checkIdx(i);
        boolean notAllowNull = checkNotAllowNull(i);
        if (notAllowNull) {
            throw new SQLException("this column not allow null");
        }
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, null);
    }

    @Override
    public void setNull(int i, int i1) throws SQLException {
        setNull(i);
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeBool);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, b);
    }

    @Override
    @Deprecated
    public void setByte(int i, byte b) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void setShort(int i, short i1) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeInt16);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, i1);
    }

    @Override
    public void setInt(int i, int i1) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeInt32);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, i1);

    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeInt64);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, l);
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeFloat);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, v);
    }

    @Override
    public void setDouble(int i, double v) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeDouble);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, v);
    }

    @Override
    @Deprecated
    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        throw new SQLException("current do not support this type");
    }

    private boolean checkNotAllowNull(int i) {
        return this.currentSchema.IsColumnNotNull(i - 1);
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeString);
        if (s == null) {
            setNull(i);
            return;
        }
        byte[] bytes = s.getBytes(CHARSET);
        stringsLen.put(i, bytes.length);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, bytes);
    }

    @Override
    @Deprecated
    public void setBytes(int i, byte[] bytes) throws SQLException {
        throw new SQLException("current do not support this type");
    }

    @Override
    public void setDate(int i, java.sql.Date date) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeDate);
        if (date == null) {
            setNull(i);
            return;
        }
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, date);
    }

    @Override
    @Deprecated
    public void setTime(int i, Time time) throws SQLException {
        throw new SQLException("current do not support this type");
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeTimestamp);
        if (timestamp == null) {
            setNull(i);
            return;
        }
        hasSet.set(i - 1, true);
        long ts = timestamp.getTime();
        currentDatas.set(i - 1, ts);
    }

    @Override
    @Deprecated
    public void setAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
        throw new SQLException("current do not support this type");
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int i, InputStream inputStream, int i1) throws SQLException {
        throw new SQLException("current do not support this type");
    }

    @Override
    @Deprecated
    public void setBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
        throw new SQLException("current do not support this type");
    }

    @Override
    public void clearParameters() {
        for (int i = 0; i < hasSet.size(); i++) {
            hasSet.set(i, false);
            currentDatas.set(i, null);
        }
        stringsLen.clear();
    }

    @Override
    @Deprecated
    public void setObject(int i, Object o, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    protected void dataBuild() throws SQLException {
        if (this.currentRow == null) {
            throw new SQLException("currentRow is null");
        }
        for (int i = 0; i < this.hasSet.size(); i++) {
            if (!this.hasSet.get(i)) {
                throw new SQLException("data not enough, index is " + i);
            }
        }
        int strLen = 0;
        for (Map.Entry<Integer, Integer> entry : stringsLen.entrySet()) {
            strLen += entry.getValue();
        }
        boolean ok = this.currentRow.Init(strLen);
        if (!ok) {
            throw new SQLException("build data row failed");
        }
        for (int i = 0; i < this.currentSchema.GetColumnCnt(); i++) {
            DataType dataType = this.currentSchema.GetColumnType(i);
            Object data = this.currentDatas.get(i);
            if (data == null) {
                ok = this.currentRow.AppendNULL();
            } else {
                if (DataType.kTypeBool.equals(dataType)) {
                    ok = this.currentRow.AppendBool((boolean) data);
                } else if (DataType.kTypeDate.equals(dataType)) {
                    java.sql.Date date = (java.sql.Date) data;
                    ok = this.currentRow.AppendDate(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                } else if (DataType.kTypeDouble.equals(dataType)) {
                    ok = this.currentRow.AppendDouble((double) data);
                } else if (DataType.kTypeFloat.equals(dataType)) {
                    ok = this.currentRow.AppendFloat((float) data);
                } else if (DataType.kTypeInt16.equals(dataType)) {
                    ok = this.currentRow.AppendInt16((short) data);
                } else if (DataType.kTypeInt32.equals(dataType)) {
                    ok = this.currentRow.AppendInt32((int) data);
                } else if (DataType.kTypeInt64.equals(dataType)) {
                    ok = this.currentRow.AppendInt64((long) data);
                } else if (DataType.kTypeString.equals(dataType)) {
                    byte[] bdata = (byte[])data;
                    ok = this.currentRow.AppendString(bdata, bdata.length);
                } else if (DataType.kTypeTimestamp.equals(dataType)) {
                    ok = this.currentRow.AppendTimestamp((long) data);
                } else {
                    throw new SQLException("unkown data type " + dataType.toString());
                }
            }
            if (!ok) {
                throw new SQLException("append data failed, idx is " + i);
            }
        }
        if (!this.currentRow.Build()) {
            throw new SQLException("build request row failed");
        }
        clearParameters();
    }

    @Override
    @Deprecated
    public void setObject(int i, Object o) throws SQLException {
        throw new SQLException("current do not support this m¡ethod");
    }

    @Override
    @Deprecated
    public boolean execute() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setCharacterStream(int i, Reader reader, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setRef(int i, Ref ref) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setBlob(int i, Blob blob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setClob(int i, Clob clob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setArray(int i, Array array) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        checkNull();
        return new SQLResultSetMetaData(this.currentSchema);
    }

    @Override
    @Deprecated
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setNull(int i, int i1, String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setURL(int i, URL url) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setRowId(int i, RowId rowId) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setNString(int i, String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setNClob(int i, NClob nClob) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setClob(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setNClob(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setObject(int i, Object o, int i1, int i2) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setCharacterStream(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setClob(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setBlob(int i, InputStream inputStream) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setNClob(int i, Reader reader) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public java.sql.ResultSet executeQuery(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int executeUpdate(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void close() throws SQLException {
        this.db = null;
        this.currentSql = null;
        this.router = null;
        if (this.currentSchema != null) {
            this.currentSchema.delete();
            this.currentSchema = null;
        }
        this.currentDatas = null;
        this.hasSet = null;
        this.stringsLen = null;
        if (this.currentRow != null) {
            this.currentRow.delete();
            this.currentRow = null;
        }
        this.closed = true;
    }

    @Override
    @Deprecated
    public int getMaxFieldSize() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setMaxFieldSize(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getMaxRows() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setMaxRows(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setEscapeProcessing(boolean b) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getQueryTimeout() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setQueryTimeout(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void cancel() throws SQLException {
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
    public void setCursorName(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean execute(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public java.sql.ResultSet getResultSet() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getUpdateCount() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean getMoreResults() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void setFetchDirection(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Deprecated
    @Override
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
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getResultSetType() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void addBatch(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public Connection getConnection() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean getMoreResults(int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int executeUpdate(String s, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int executeUpdate(String s, int[] ints) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int executeUpdate(String s, String[] strings) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean execute(String s, int i) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean execute(String s, int[] ints) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean execute(String s, String[] strings) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    @Deprecated
    public void setPoolable(boolean b) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public boolean isPoolable() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.closeOnComplete = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return this.closeOnComplete;
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


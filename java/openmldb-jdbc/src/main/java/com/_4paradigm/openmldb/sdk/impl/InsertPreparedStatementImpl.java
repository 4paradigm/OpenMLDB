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

import com._4paradigm.openmldb.*;

import com._4paradigm.openmldb.common.Pair;
import com._4paradigm.openmldb.jdbc.SQLInsertMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class InsertPreparedStatementImpl implements PreparedStatement {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final Logger logger = LoggerFactory.getLogger(InsertPreparedStatementImpl.class);

    private final String db;
    private final String sql;
    private final SQLRouter router;

    // need manual deletion
    private final List<SQLInsertRow> currentRows = new ArrayList<>();
    private Schema currentSchema;

    private final List<Object> currentDatas;
    private final List<DataType> currentDatasType;
    private final List<Boolean> hasSet;
    // stmt insert idx -> real table schema idx
    private final List<Pair<Long, Integer>> schemaIdxes;
    // used by building row
    private final List<Pair<Long, Integer>> sortedIdxes;

    private boolean closed = false;
    private boolean closeOnComplete = false;
    private Integer stringsLen = 0;

    public InsertPreparedStatementImpl(String db, String sql, SQLRouter router) throws SQLException {
        this.db = db;
        this.sql = sql;
        this.router = router;

        SQLInsertRow tempRow = getSQLInsertRow();
        this.currentSchema = tempRow.GetSchema();
        VectorUint32 idxes = tempRow.GetHoleIdx();

        // In stmt order, if no columns in stmt, in schema order
        // We'll sort it to schema order later, so needs the map <real_schema_idx, current_data_idx>
        schemaIdxes = new ArrayList<>(idxes.size());
        // CurrentData and Type order is consistent with insert stmt. We'll do appending in schema order when build
        // row.
        currentDatas = new ArrayList<>(idxes.size());
        currentDatasType = new ArrayList<>(idxes.size());
        hasSet = new ArrayList<>(idxes.size());

        for (int i = 0; i < idxes.size(); i++) {
            Long realIdx = idxes.get(i);
            schemaIdxes.add(new Pair<>(realIdx, i));
            DataType type = currentSchema.GetColumnType(realIdx);
            currentDatasType.add(type);
            currentDatas.add(null);
            hasSet.add(false);
            logger.debug("add col {}, {}", currentSchema.GetColumnName(realIdx), type);
        }
        // SQLInsertRow::AppendXXX order is the schema order(skip the no-hole columns)
        sortedIdxes = schemaIdxes.stream().sorted(Comparator.comparing(Pair::getKey))
                .collect(Collectors.toList());
    }

    private SQLInsertRow getSQLInsertRow() throws SQLException {
        Status status = new Status();
        SQLInsertRow row = router.GetInsertRow(db, sql, status);
        if (status.getCode() != 0) {
            String msg = status.ToString();
            status.delete();
            if (row != null) {
                row.delete();
            }
            throw new SQLException("getSQLInsertRow failed, " + msg);
        }
        status.delete();
        return row;
    }

    private void clearSQLInsertRowList() {
        for (SQLInsertRow row : currentRows) {
            row.delete();
        }
        currentRows.clear();
    }

    @Override
    @Deprecated
    public ResultSet executeQuery() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int executeUpdate() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    private void checkIdx(int i) throws SQLException {
        if (closed) {
            throw new SQLException("prepared statement closed");
        }
        if (i <= 0) {
            throw new SQLException("error sqe number");
        }
        if (i > schemaIdxes.size()) {
            throw new SQLException("out of data range");
        }
    }

    private void checkType(int i, DataType type) throws SQLException {
        if (currentDatasType.get(i - 1) != type) {
            throw new SQLException("data type not match, expect " + currentDatasType.get(i - 1) + ", actual " + type);
        }
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
        Long idx = this.schemaIdxes.get(i - 1).getKey();
        return this.currentSchema.IsColumnNotNull(idx);
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
        // if this index already set, should first reduce length of bytes last time
        if (hasSet.get(i - 1)) {
            stringsLen -= ((byte[]) currentDatas.get(i - 1)).length;
        }
        stringsLen += bytes.length;
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, bytes);
    }

    @Override
    @Deprecated
    public void setBytes(int i, byte[] bytes) throws SQLException {
        throw new SQLException("current do not support this type");
    }

    @Override
    public void setDate(int i, Date date) throws SQLException {
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
    public void clearParameters() throws SQLException {
        for (int i = 0; i < hasSet.size(); i++) {
            hasSet.set(i, false);
            currentDatas.set(i, null);
        }
        stringsLen = 0;
    }

    @Override
    @Deprecated
    public void setObject(int i, Object o, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    private void buildRow() throws SQLException {
        SQLInsertRow currentRow = getSQLInsertRow();
        boolean ok = currentRow.Init(stringsLen);
        if (!ok) {
            throw new SQLException("init row failed");
        }

        for (Pair<Long, Integer> sortedIdx : sortedIdxes) {
            Integer currentDataIdx = sortedIdx.getValue();
            Object data = currentDatas.get(currentDataIdx);
            if (data == null) {
                ok = currentRow.AppendNULL();
            } else {
                DataType curType = currentDatasType.get(currentDataIdx);
                if (DataType.kTypeBool.equals(curType)) {
                    ok = currentRow.AppendBool((boolean) data);
                } else if (DataType.kTypeDate.equals(curType)) {
                    Date date = (Date) data;
                    ok = currentRow.AppendDate(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
                } else if (DataType.kTypeDouble.equals(curType)) {
                    ok = currentRow.AppendDouble((double) data);
                } else if (DataType.kTypeFloat.equals(curType)) {
                    ok = currentRow.AppendFloat((float) data);
                } else if (DataType.kTypeInt16.equals(curType)) {
                    ok = currentRow.AppendInt16((short) data);
                } else if (DataType.kTypeInt32.equals(curType)) {
                    ok = currentRow.AppendInt32((int) data);
                } else if (DataType.kTypeInt64.equals(curType)) {
                    ok = currentRow.AppendInt64((long) data);
                } else if (DataType.kTypeString.equals(curType)) {
                    byte[] bdata = (byte[]) data;
                    ok = currentRow.AppendString(bdata, bdata.length);
                } else if (DataType.kTypeTimestamp.equals(curType)) {
                    ok = currentRow.AppendTimestamp((long) data);
                } else {
                    throw new SQLException("unknown data type");
                }
            }
            if (!ok) {
                throw new SQLException("append failed on currentDataIdx: " + currentDataIdx + ", curType: " + currentDatasType.get(currentDataIdx) + ", current data: " + data);
            }
        }
        if (!currentRow.Build()) {
            throw new SQLException("build insert row failed(str size init != actual)");
        }
        currentRows.add(currentRow);
        clearParameters();
    }

    @Override
    @Deprecated
    public void setObject(int i, Object o) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public boolean execute() throws SQLException {
        if (closed) {
            throw new SQLException("InsertPreparedStatement closed");
        }
        // buildRow will add a new row to currentRows
        if (!currentRows.isEmpty()) {
            throw new SQLException("please use executeBatch");
        }
        buildRow();
        Status status = new Status();
        // actually only one row
        boolean ok = router.ExecuteInsert(db, sql, currentRows.get(0), status);
        // cleanup rows even if insert failed
        // we can't execute() again without set new row, so we must clean up here
        clearSQLInsertRowList();
        if (!ok) {
            logger.error("execute insert failed: {}", status.ToString());
            status.delete();
            return false;
        }

        status.delete();
        if (closeOnComplete) {
            close();
        }
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        if (closed) {
            throw new SQLException("InsertPreparedStatement closed");
        }
        // build the current row and cleanup the cache of current row
        // so that the cache is ready for new row
        buildRow();
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
        return new SQLInsertMetaData(this.currentDatasType, this.currentSchema, this.schemaIdxes);
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
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
    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
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
    public ResultSet executeQuery(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int executeUpdate(String s) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        clearSQLInsertRowList();
        if (currentSchema != null) {
            currentSchema.delete();
            currentSchema = null;
        }
        closed = true;
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
    public ResultSet getResultSet() throws SQLException {
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
    public void setFetchSize(int i) throws SQLException {
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
        throw new SQLException("cannot take arguments in PreparedStatement");
    }

    @Override
    @Deprecated
    public void clearBatch() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (closed) {
            throw new SQLException("InsertPreparedStatement closed");
        }
        int[] result = new int[currentRows.size()];
        Status status = new Status();
        for (int i = 0; i < currentRows.size(); i++) {
            boolean ok = router.ExecuteInsert(db, sql, currentRows.get(i), status);
            if (!ok) {
                // TODO(hw): may lost log, e.g. openmldb-batch online import in yarn mode?
                logger.warn(status.ToString());
            }
            result[i] = ok ? 0 : -1;
        }
        status.delete();
        clearSQLInsertRowList();
        return result;
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

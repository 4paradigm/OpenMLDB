package com._4paradigm.sql.sdk;

import com._4paradigm.sql.*;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.sql.ResultSet;
import java.util.*;

public class InsertPreparedStatementImpl implements PreparedStatement {
    private String currentSql;
    private SQLInsertRow currentRow = null;
    private SQLInsertRows currentRows = null;
    private SQLRouter router = null;
    private List<Object> currentDatas = null;
    private List<DataType> currentDatasType = null;
    private Schema currentSchema = null;
    private String db;
    private List<Boolean> hasSet;
    private static final Logger logger = LoggerFactory.getLogger(SqlClusterExecutor.class);
    private boolean closed = false;
    private boolean closeOnComplete = false;
    private Map<String, SQLInsertRows> sqlRowsMap = new HashMap<>();

    public InsertPreparedStatementImpl(String db, String sql, SQLRouter router) throws SqlException {
        Status status = new Status();
        SQLInsertRows rows = router.GetInsertRows(db, sql, status);
        if (status.getCode() != 0) {
            String msg = status.getMsg();
            logger.error("getInsertRows fail: {}", msg);
            throw new SqlException("get insertrows fail " + msg + " in construction preparedstatement");
        }
        status.delete();
        this.currentRows = rows;
        this.currentRow = rows.NewRow();
        this.currentSql = sql;
        this.router = router;
        this.currentSql = sql;
        currentSchema = this.currentRow.GetSchema();
        this.db = db;
        VectorUint32 idxs = this.currentRow.GetHoleIdx();
        currentDatas = new ArrayList<>(idxs.size());
        currentDatasType = new ArrayList<>(idxs.size());
        hasSet = new ArrayList<>(idxs.size());
        for (int i = 0; i < idxs.size(); i++) {
            long idx = idxs.get(i);
            DataType type = currentSchema.GetColumnType(idx);
            currentDatasType.add(type);
            currentDatas.add(null);
            hasSet.add(false);
        }
        idxs.delete();
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
            throw new SQLException("preparedstatement closed");
        }
        if (i > currentDatasType.size()) {
            throw new SQLException("out of data range");
        }
    }

    private void checkType(int i, DataType type) throws SQLException {
        if (currentDatasType.get(i - 1) != type) {
            throw new SQLException("data type not match");
        }
    }

    @Override
    public void setNull(int i, int i1) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeBool);
        if (!this.currentSchema.IsColumnNotNull(i)) {
            throw new SQLException("column not allow null");
        }
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, null);
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

    @Override
    public void setString(int i, String s) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeString);
        hasSet.set(i - 1, true);
        currentDatas.set(i - 1, s);
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
        hasSet.set(i - 1, true);
        int year = date.getYear();
        int month = date.getMonth();
        int day = date.getDate();
        int data = year << 16;
        data = data | (month << 8);
        data = data | day;
        currentDatas.set(i - 1, data);

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
    }

    @Override
    @Deprecated
    public void setObject(int i, Object o, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    private void dataBuild() throws SQLException {
        if (currentRows == null) {
            throw new SQLException("null rows");
        }
        if (currentRow == null) {
            currentRow = currentRows.NewRow();
        }
        int strLen = 0;
        for (int i = 0; i < currentDatasType.size(); i++) {
            if (currentDatasType.get(i) != DataType.kTypeString) {
                continue;
            }
            strLen = ((String)currentDatas.get(i)).length();
        }
        currentRow.Init(strLen);
        for (int i = 0; i < currentDatasType.size(); i++) {
            Object data = currentDatas.get(i);
            if (data == null) {
                currentRow.AppendNULL();
            } else if (DataType.kTypeBool.equals(currentDatasType.get(i))) {
                currentRow.AppendBool((boolean) data);
            } else if (DataType.kTypeDate.equals(currentDatasType.get(i))) {
                currentRow.AppendDate((int) data);
            } else if (DataType.kTypeDouble.equals(currentDatasType.get(i))) {
                currentRow.AppendDouble((double) data);
            } else if (DataType.kTypeFloat.equals(currentDatasType.get(i))) {
                currentRow.AppendFloat((float) data);
            } else if (DataType.kTypeInt16.equals(currentDatasType.get(i))) {
                currentRow.AppendInt16((short) data);
            } else if (DataType.kTypeInt32.equals(currentDatasType.get(i))) {
                currentRow.AppendInt32((int) data);
            } else if (DataType.kTypeInt64.equals(currentDatasType.get(i))) {
                currentRow.AppendInt64((long) data);
            } else if (DataType.kTypeString.equals(currentDatasType.get(i))) {
                currentRow.AppendString((String) data);
            } else if (DataType.kTypeTimestamp.equals(currentDatasType.get(i))) {
                currentRow.AppendTimestamp((long) data);
            } else {
                throw new SQLException("unkown data type");
            }
        }
        boolean ok = currentRow.IsComplete();
        if (!ok) {
            throw new SQLException("all data has put, but data does not enough");
        }
        currentRow = null;
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
            throw new SQLException("preparedstatement closed");
        }
        if (!sqlRowsMap.isEmpty()) {
            throw new SQLException("please use executeBatch");
        }
        for (int i = 0; i < hasSet.size(); i++) {
            if (!hasSet.get(i)) {
                throw new SQLException("data not enough");
            }
        }
        dataBuild();
        Status status = new Status();
        boolean ok = router.ExecuteInsert(db, currentSql, currentRows, status);
        if (!ok) {
            logger.error("getInsertRow fail: {}", status.getMsg());
            return false;
        }
        if (closeOnComplete) {
            closed = true;
        }
        return true;
    }

    @Override
    @Deprecated
    public void addBatch() throws SQLException {
        if (closed) {
            throw new SQLException("preparedstatement closed");
        }
        for (int i = 0; i < hasSet.size(); i++) {
            if (!hasSet.get(i)) {
                throw new SQLException("data not enough");
            }
        }
        dataBuild();
        clearParameters();
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
    @Deprecated
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("current do not support this method");
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
        this.currentRow.delete();
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
    @Deprecated
    public void addBatch(String s) throws SQLException {
        if (s.equals(this.currentSql) && currentDatas.size() > 0) {
            throw new SQLException("data not enough");
        }
        if (sqlRowsMap.get(s) != null) {
            return;
        }
        if (s.contains("?")) {
            throw new SQLException("this sql can not direct into batch, because that contains ?");
        }
        Status status = new Status();
        SQLInsertRows rows = router.GetInsertRows(db, s, status);
        if (status.getCode() != 0) {
            String msg = status.getMsg();
            logger.error("getInsertRows fail: {}", msg);
            status.delete();
            throw new SQLException("get insertrows fail " + msg + " in construction preparedstatement");
        }
        status.delete();
        sqlRowsMap.put(s, rows);
    }

    @Override
    @Deprecated
    public void clearBatch() throws SQLException {
        for (String key : sqlRowsMap.keySet()) {
            SQLInsertRows rows = sqlRowsMap.get(key);
            rows.delete();
        }
        sqlRowsMap.clear();
        currentRow.delete();
        currentRows.delete();
        currentRow = null;
    }

    @Override
    @Deprecated
    public int[] executeBatch() throws SQLException {
        //return new int[0];
        if (closed) {
            throw new SQLException("preparedstatement closed");
        }
        int result[] = new int[1+sqlRowsMap.size()];
        Status status = new Status();
        boolean ok = router.ExecuteInsert(db, currentSql, currentRows, status);
        if (!ok) {
            result[0] = -1;
        } else {
            result[0] = 0;
        }
        int i = 1;
        for (String sql : sqlRowsMap.keySet()) {
            ok = router.ExecuteInsert(db, sql, sqlRowsMap.get(sql), status);
            if (!ok) {
                result[i] = -1;
            } else {
                result[i] = 0;
            }
            i++;
        }
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

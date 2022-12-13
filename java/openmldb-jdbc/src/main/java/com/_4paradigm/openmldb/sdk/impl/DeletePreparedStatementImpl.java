package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.SQLDeleteRow;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;

public class DeletePreparedStatementImpl implements PreparedStatement {

    private final String db;
    private final String sql;
    private final SQLRouter router;
    private final List<SQLDeleteRow> currentRows = new ArrayList<>();
    private int rowIdx;
    private boolean closed;

    public DeletePreparedStatementImpl(String db, String sql, SQLRouter router) throws SQLException {
        this.db = db;
        this.sql = sql;
        this.router = router;
        currentRows.add(getSQLDeleteRow());
        rowIdx = 0;
        this.closed = false;
    }

    private SQLDeleteRow getSQLDeleteRow() throws SQLException {
        Status status = new Status();
        SQLDeleteRow row = router.GetDeleteRow(db, sql, status);
        if (!status.IsOK()) {
            String msg = status.ToString();
            status.delete();
            if (row != null) {
                row.delete();
            }
            throw new SQLException("getSQLDeleteRow failed, " + msg);
        }
        status.delete();
        return row;
    }

    @Override
    @Deprecated
    public ResultSet executeQuery() throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (!currentRows.get(0).Build()) {
            throw new SQLException("fail to build row");
        }
        Status status = new Status();
        router.ExecuteDelete(currentRows.get(0), status);
        if (!status.IsOK()) {
            String msg = status.ToString();
            status.delete();
            throw new SQLException(msg);
        }
        status.delete();
        return 0;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        currentRows.get(rowIdx).SetNULL(parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        currentRows.get(rowIdx).SetBool(parameterIndex, x);
    }

    @Override
    @Deprecated
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        currentRows.get(rowIdx).SetInt(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        currentRows.get(rowIdx).SetInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        currentRows.get(rowIdx).SetInt(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new SQLException("cannot delete by float column");
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new SQLException("cannot delete by double column");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new SQLException("cannot delete by decimal column");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        currentRows.get(rowIdx).SetString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        currentRows.get(rowIdx).SetDate(parameterIndex, x.getYear() + 1900, x.getMonth() + 1, x.getDate());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLException("cannot delete by date column");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        currentRows.get(rowIdx).SetInt(parameterIndex, x.getTime());
    }

    @Override
    @Deprecated
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    @Deprecated
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void clearParameters() throws SQLException {
        currentRows.get(rowIdx).Reset();
    }

    @Override
    @Deprecated
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setObject(int parameterIndex, Object x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    public boolean execute() throws SQLException {
        executeUpdate();
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
        rowIdx++;
        if (rowIdx >= currentRows.size()) {
            currentRows.add(getSQLDeleteRow());
        }
    }

    @Deprecated
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    @Deprecated
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    @Deprecated
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    @Deprecated
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("unsupport this method");
    }

    @Override
    @Deprecated
    public ResultSet executeQuery(String sql) throws SQLException {
        return null;
    }

    @Override
    @Deprecated
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public void close() throws SQLException {
        for (SQLDeleteRow row : currentRows) {
            row.delete();
        }
        currentRows.clear();
        closed = true;
    }

    @Override
    @Deprecated
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    @Deprecated
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    @Deprecated
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    @Deprecated
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    @Deprecated
    public void cancel() throws SQLException {

    }

    @Override
    @Deprecated
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    @Deprecated
    public void clearWarnings() throws SQLException {

    }

    @Override
    @Deprecated
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    @Deprecated
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    @Deprecated
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    @Deprecated
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    @Deprecated
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    @Deprecated
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    @Deprecated
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {
        rowIdx = 0;
        for (SQLDeleteRow row : currentRows) {
            row.Reset();
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int[] result = new int[rowIdx];
        for (int idx = 0; idx < rowIdx; idx++) {
            if (!currentRows.get(idx).Build()) {
                result[idx] = EXECUTE_FAILED;
                continue;
            }
            Status status = new Status();
            router.ExecuteDelete(currentRows.get(idx), status);
            if (status.IsOK()) {
                result[idx] = 0;
            } else {
                result[idx] = EXECUTE_FAILED;
            }
            status.delete();
        }
        clearBatch();
        return result;
    }

    @Override
    @Deprecated
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    @Deprecated
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    @Deprecated
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    @Deprecated
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    @Deprecated
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    @Deprecated
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    @Deprecated
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    @Deprecated
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    @Deprecated
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    @Deprecated
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    @Deprecated
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    @Deprecated
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}

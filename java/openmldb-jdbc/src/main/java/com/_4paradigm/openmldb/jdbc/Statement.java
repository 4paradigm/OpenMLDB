package com._4paradigm.openmldb.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

public class Statement implements java.sql.Statement {
    private ResultSet resultSet;
    private final SQLConnection connection;
    private boolean closed;

    public Statement(SQLConnection connection) {
        this.connection = connection;
        this.closed = false;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        throw new SQLException("unsupported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (resultSet == null) {
            throw new SQLException("no result set");
        }
        return resultSet;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        resultSet = connection.client.executeSQL(connection.defaultDatabase, sql);
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        throw new SQLException("unsupported");
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
        closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("statement closed");
        }
    }

    @Override
    @Deprecated
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int executeUpdate(String sql, int columnIndexes[]) throws SQLException {
        throw new UnsupportedOperationException("executeUpdate not implemented");
    }

    @Override
    @Deprecated
    public int executeUpdate(String sql, String columnNames[]) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean execute(String sql, int columnIndexes[]) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean execute(String sql, String columnNames[]) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }

    @Override
    @Deprecated
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        throw new UnsupportedOperationException("method not implemented");
    }
}

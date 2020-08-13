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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class InsertPreparedStatementImpl implements PreparedStatement {
    private String sql;
    private SQLInsertRow row;
    private SQLInsertRows rows;
    private SQLRouter router;
    private List<Object> datas = null;
    private List<DataType> datasType = null;
    private Schema schema = null;
    private String db;
    private List<Boolean> hasSet;
    private static final Logger logger = LoggerFactory.getLogger(SqlClusterExecutor.class);
    private boolean closed = false;
    private boolean closeOnComplete = false;

    public InsertPreparedStatementImpl(String db, SQLInsertRow row, String sql, SQLRouter router) {
        this.row = row;
        this.sql = sql;
        this.router = router;
        this.schema = row.GetSchema();
        this.db = db;
        VectorUint32 idxs = row.GetHoleIdx();
        datas = new ArrayList<>(idxs.size());
        datasType = new ArrayList<>(idxs.size());
        hasSet = new ArrayList<>(idxs.size());
        for (int i = 0; i < idxs.size(); i++) {
            long idx = idxs.get(i);
            DataType type = schema.GetColumnType(idx);
            datasType.add(type);
            datas.add(null);
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
        if (i > datasType.size()) {
            throw new SQLException("out of data range");
        }
    }

    private void checkType(int i, DataType type) throws SQLException {
        if (datasType.get(i - 1) != type) {
            throw new SQLException("data type not match");
        }
    }

    @Override
    public void setNull(int i, int i1) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeBool);
        if (!this.schema.IsColumnNotNull(i)) {
            throw new SQLException("column not allow null");
        }
        hasSet.set(i - 1, true);
        datas.set(i - 1, null);
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeBool);
        hasSet.set(i - 1, true);
        datas.set(i - 1, b);
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
        datas.set(i - 1, i1);
    }

    @Override
    public void setInt(int i, int i1) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeInt32);
        hasSet.set(i - 1, true);
        datas.set(i - 1, i1);

    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeInt64);
        hasSet.set(i - 1, true);
        datas.set(i - 1, l);
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeFloat);
        hasSet.set(i - 1, true);
        datas.set(i - 1, v);
    }

    @Override
    public void setDouble(int i, double v) throws SQLException {
        checkIdx(i);
        checkType(i, DataType.kTypeDouble);
        hasSet.set(i - 1, true);
        datas.set(i - 1, v);
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
        datas.set(i - 1, s);
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
        datas.set(i - 1, data);

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
        datas.set(i - 1, ts);
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
            datas.set(i, null);
        }
    }

    @Override
    @Deprecated
    public void setObject(int i, Object o, int i1) throws SQLException {
        throw new SQLException("current do not support this method");
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
        for (int i = 0; i < hasSet.size(); i++) {
            if (!hasSet.get(i)) {
                throw new SQLException("data not enough");
            }
        }
        int strLen = 0;
        for (int i = 0; i < datasType.size(); i++) {
            if (datasType.get(i) != DataType.kTypeString) {
                continue;
            }
            strLen += ((String)datas.get(i)).length();
        }
        row.Init(strLen);
        for (int i = 0; i < datasType.size(); i++) {
            Object data = datas.get(i);
            if (data == null) {
                row.AppendNULL();
            } else if (DataType.kTypeBool.equals(datasType.get(i))) {
                row.AppendBool((boolean) data);
            } else if (DataType.kTypeDate.equals(datasType.get(i))) {
                row.AppendDate((int) data);
            } else if (DataType.kTypeDouble.equals(datasType.get(i))) {
                row.AppendDouble((double) data);
            } else if (DataType.kTypeFloat.equals(datasType.get(i))) {
                row.AppendFloat((float) data);
            } else if (DataType.kTypeInt16.equals(datasType.get(i))) {
                row.AppendInt16((short) data);
            } else if (DataType.kTypeInt32.equals(datasType.get(i))) {
                row.AppendInt32((int) data);
            } else if (DataType.kTypeInt64.equals(datasType.get(i))) {
                row.AppendInt64((long) data);
            } else if (DataType.kTypeString.equals(datasType.get(i))) {
                row.AppendString((String) data);
            } else if (DataType.kTypeTimestamp.equals(datasType.get(i))) {
                row.AppendTimestamp((long) data);
            } else {
                throw new SQLException("unkown data type");
            }
        }
        boolean ok = row.IsComplete();
        if (!ok) {
            throw new SQLException("all data has put, but data does not enough");
        }
        Status status = new Status();
        ok = router.ExecuteInsert(db, sql, row, status);
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
        this.row.delete();
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
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public void clearBatch() throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    @Deprecated
    public int[] executeBatch() throws SQLException {
        //return new int[0];
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

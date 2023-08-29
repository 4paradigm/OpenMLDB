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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.jdbc.PreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.*;
import com._4paradigm.openmldb.jdbc.SQLResultSetMetaData;
import com._4paradigm.openmldb.sdk.Common;

public class PreparedStatementImpl extends PreparedStatement  {
    private String db;
    private String currentSql;
    private SQLRequestRow currentRow;
    private Schema currentSchema;
    private TreeMap<Integer, com._4paradigm.openmldb.DataType> types;
    private TreeMap<Integer, com._4paradigm.openmldb.DataType> orgTypes;
    private TreeMap<Integer, Object> currentDatas;
    private Map<Integer, Integer> stringsLen = new HashMap<>();
    public static final Charset CHARSET = StandardCharsets.UTF_8;


    public PreparedStatementImpl(String db, String sql, SQLRouter router) throws SQLException {
        if (db == null) throw new SQLException("db is null");
        if (router == null) throw new SQLException("router is null");
        if (sql == null) throw new SQLException("spName is null");
        this.db = db;
        this.currentSql = sql;
        this.router = router;
        this.currentDatas = new TreeMap<Integer, Object>();
        this.types = new TreeMap<Integer, DataType>();
    }

    private void checkNull() throws SQLException {
        if (db == null) {
            throw new SQLException("db is null");
        }
        if (currentSql == null) {
            throw new SQLException("sql is null");
        }
        if (router == null) {
            throw new SQLException("SQLRouter is null");
        }
        if (currentDatas == null) {
            throw new SQLException("currentDatas is null");
        }
        if (types == null) {
            throw new SQLException("currentDatas is null");
        }
    }

    void checkIdx(int i) throws SQLException {
        checkClosed();
        checkNull();
        if (i <= 0) {
            throw new SQLException("index out of array");
        }
        if (currentDatas.containsKey(i)) {
            throw new SQLException("index duplicate, index: " + i + " already exist");
        }
    }

    @Override
    public SQLResultSet executeQuery() throws SQLException {
        checkClosed();
        checkExecutorClosed();
        dataBuild();
        Status status = new Status();
        com._4paradigm.openmldb.ResultSet resultSet = router.ExecuteSQLParameterized(db, currentSql, currentRow, status);
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

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex, Util.sqlTypeToDataType(sqlType));
    }

    private void setNull(int i, DataType type) throws SQLException {
        checkIdx(i);
        types.put(i, type);
        currentDatas.put(i, null);
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        checkIdx(i);
        types.put(i, DataType.kTypeBool);
        currentDatas.put(i, b);
    }

    @Override
    public void setShort(int i, short i1) throws SQLException {
        checkIdx(i);
        types.put(i, DataType.kTypeInt16);
        currentDatas.put(i, i1);
    }

    @Override
    public void setInt(int i, int i1) throws SQLException {
        checkIdx(i);
        types.put(i, DataType.kTypeInt32);
        currentDatas.put(i, i1);
    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        checkIdx(i);
        types.put(i, DataType.kTypeInt64);
        currentDatas.put(i, l);
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        checkIdx(i);
        types.put(i, DataType.kTypeFloat);
        currentDatas.put(i, v);
    }

    @Override
    public void setDouble(int i, double v) throws SQLException {
        checkIdx(i);
        types.put(i, DataType.kTypeDouble);
        currentDatas.put(i, v);
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        checkIdx(i);
        if (s == null) {
            setNull(i, DataType.kTypeString);
            return;
        }
        types.put(i, DataType.kTypeString);
        byte[] bytes = s.getBytes(CHARSET);
        stringsLen.put(i, bytes.length);
        currentDatas.put(i, bytes);
    }

    @Override
    public void setDate(int i, java.sql.Date date) throws SQLException {
        checkIdx(i);
        if (date == null) {
            setNull(i, DataType.kTypeDate);
            return;
        }
        types.put(i, DataType.kTypeDate);
        currentDatas.put(i, date);
    }

    @Override
    public void setTimestamp(int i, java.sql.Timestamp timestamp) throws SQLException {
        checkIdx(i);
        if (timestamp == null) {
            setNull(i, DataType.kTypeTimestamp);
            return;
        }
        types.put(i, DataType.kTypeTimestamp);
        long ts = timestamp.getTime();
        currentDatas.put(i, ts);
    }

    @Override
    public void clearParameters() {
        currentDatas.clear();
        types.clear();
        stringsLen.clear();
    }

    protected void dataBuild() throws SQLException {
        if (types == null) {
            throw new SQLException("fail to build data when data types is null");
        }
        // types has been updated
        if (null == this.currentRow || orgTypes != types) {
            if (types.firstKey() != 1 || types.lastKey() != types.size()) {
                throw new SQLException("data not enough, indexes are " + currentDatas.keySet());
            }
            ColumnTypes columnTypes = new ColumnTypes();
            for (int i = 0; i < types.size(); i++) {
                columnTypes.AddColumnType(types.get(i + 1));
            }
            this.currentRow = SQLRequestRow.CreateSQLRequestRowFromColumnTypes(columnTypes);
            if (this.currentRow == null) {
                throw new SQLException("fail to create sql request row from column types");
            }
            this.currentSchema = this.currentRow.GetSchema();
            this.orgTypes = this.types;
        }
        if (this.currentSchema == null) {
            throw new SQLException("fail to build data with null schema");
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
            Object data = this.currentDatas.get(i + 1);
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
                    byte[] bdata = (byte[]) data;
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
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        checkNull();
        return new SQLResultSetMetaData(Common.convertSchema(this.currentSchema));
    }
}

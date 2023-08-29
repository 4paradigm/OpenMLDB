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

import com._4paradigm.openmldb.common.codec.CodecUtil;
import com._4paradigm.openmldb.common.codec.FlexibleRowBuilder;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.QueryFuture;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BatchCallablePreparedStatementImpl extends CallablePreparedStatement {

    private List<ByteBuffer> datas = new ArrayList<>();
    private ByteBuffer meta;
    private ByteBuffer result;
    private int totalSize = 0;

    public BatchCallablePreparedStatementImpl(Deployment deployment, SQLRouter router) throws SQLException {
        super(deployment, router);
        rowBuilder = new FlexibleRowBuilder(deployment.getInputMetaData());
    }

    private void build() throws SQLException {
        if (datas.isEmpty()) {
            throw new SQLException("no data");
        }
        meta = ByteBuffer.allocateDirect(4 * (datas.size() + 1)).order(ByteOrder.LITTLE_ENDIAN);
        meta.putInt(0);  // reserved for common slice
        result = ByteBuffer.allocateDirect(totalSize);
        for (ByteBuffer buf : datas) {
            meta.putInt(buf.capacity());
            result.put(buf.array());
        }
    }

    @Override
    public SQLResultSet executeQuery() throws SQLException {
        checkClosed();
        checkExecutorClosed();
        build();
        Status status = new Status();
        com._4paradigm.openmldb.ResultSet resultSet = router.CallSQLBatchRequestProcedure(
                db, deploymentName, meta, meta.capacity(), result, result.capacity(), status);
        if (status.getCode() != 0 || resultSet == null) {
            String msg = status.ToString();
            status.delete();
            if (resultSet != null) {
                resultSet.delete();
            }
            throw new SQLException("execute sql fail: " + msg);
        }
        status.delete();
        SQLResultSet rs = new SQLResultSet(resultSet);
        if (closeOnComplete) {
            closed = true;
        }
        clearParameters();
        return rs;
    }

    @Override
    public QueryFuture executeQueryAsync(long timeOut, TimeUnit unit) throws SQLException {
        checkClosed();
        checkExecutorClosed();
        build();
        Status status = new Status();
        com._4paradigm.openmldb.QueryFuture queryFuture = router.CallSQLBatchRequestProcedure(db, deploymentName, unit.toMillis(timeOut),
                meta, meta.capacity(), result, result.capacity(), status);
        if (status.getCode() != 0 || queryFuture == null) {
            String msg = status.ToString();
            status.delete();
            if (queryFuture != null) {
                queryFuture.delete();
            }
            throw new SQLException("call deployment failed, msg: " + msg);
        }
        status.delete();
        clearParameters();
        return new QueryFuture(queryFuture);
    }

    @Override
    public void addBatch() throws SQLException {
        if (!rowBuilder.build()) {
            throw new SQLException("failed to encode data");
        }
        ByteBuffer buf = rowBuilder.getValue();
        datas.add(buf);
        totalSize += buf.capacity();
        rowBuilder.clear();
    }

    @Override
    public void clearBatch() throws SQLException {
        clearParameters();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException("Should use executeQuery() to get batch result");
    }

    @Override
    public void clearParameters() {
        datas.clear();
        rowBuilder.clear();
        result = null;
        meta = null;
        totalSize = 0;
    }

    @Override
    public void setNull(int i, int i1) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setNULL(realIdx)) {
            throw new SQLException("set null failed. idx is " + i);
        }
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setBool(realIdx, b)) {
            throw new SQLException("set bool failed. idx is " + i);
        }
    }

    @Override
    public void setShort(int i, short i1) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setSmallInt(realIdx, i1)) {
            throw new SQLException("set short failed. idx is " + i);
        }
    }

    @Override
    public void setInt(int i, int i1) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setInt(realIdx, i1)) {
            throw new SQLException("set int failed. idx is " + i);
        }
    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setBigInt(realIdx, l)) {
            throw new SQLException("set long failed. idx is " + i);
        }
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setFloat(realIdx, v)) {
            throw new SQLException("set float failed. idx is " + i);
        }
    }

    @Override
    public void setDouble(int i, double v) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setDouble(realIdx, v)) {
            throw new SQLException("set double failed. idx is " + i);
        }
    }

    @Override
    public void setDate(int i, java.sql.Date date) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setDate(realIdx, date)) {
            throw new SQLException("set date failed. idx is " + i);
        }
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setTimestamp(realIdx, timestamp.getTime())) {
            throw new SQLException("set timestamp failed. idx is " + i);
        }
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        int realIdx = i - 1;
        if (!rowBuilder.setString(realIdx, s)) {
            throw new SQLException("set string failed. idx is " + i);
        }
    }
}

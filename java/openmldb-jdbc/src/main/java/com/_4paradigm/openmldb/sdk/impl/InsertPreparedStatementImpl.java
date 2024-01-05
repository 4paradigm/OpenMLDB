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
import com._4paradigm.openmldb.jdbc.PreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLInsertMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.sql.Date;
import java.sql.ResultSet;
import java.util.*;

public class InsertPreparedStatementImpl extends PreparedStatement {
    private static final Logger logger = LoggerFactory.getLogger(InsertPreparedStatementImpl.class);

    private SQLRouter router;
    private FlexibleRowBuilder rowBuilder;
    private InsertPreparedStatementMeta cache;

    private Set<Integer> indexCol;
    private Map<Integer, List<Integer>> indexMap;
    private Map<Integer, String> indexValue;
    private Map<Integer, String> defaultIndexValue;
    private List<AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer>> batchValues;

    public InsertPreparedStatementImpl(InsertPreparedStatementMeta cache, SQLRouter router) throws SQLException {
        this.router = router;
        rowBuilder = new FlexibleRowBuilder(cache.getCodecMeta());
        this.cache = cache;
        indexCol = cache.getIndexPos();
        indexMap = cache.getIndexMap();
        indexValue = new HashMap<>();
        defaultIndexValue = cache.getDefaultIndexValue();
        batchValues = new ArrayList<>();
    }

    private int getSchemaIdx(int idx) throws SQLException {
        return cache.getSchemaIdx(idx - 1);
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

    private boolean setNull(int i) throws SQLException {
        if (!cache.getSchema().isNullable(i)) {
            throw new SQLException("this column not allow null");
        }
        return rowBuilder.setNULL(i);
    }

    @Override
    public void setNull(int i, int i1) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (!setNull(realIdx)) {
            throw new SQLException("set null failed. pos is " + i);
        }
        if (indexCol.contains(realIdx)) {
            indexValue.put(realIdx, InsertPreparedStatementMeta.NONETOKEN);
        }
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (!rowBuilder.setBool(realIdx, b)) {
            throw new SQLException("set bool failed. pos is " + i);
        }
        if (indexCol.contains(realIdx)) {
            indexValue.put(realIdx, String.valueOf(b));
        }
    }

    @Override
    public void setShort(int i, short i1) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (!rowBuilder.setSmallInt(realIdx, i1)) {
            throw new SQLException("set short failed. pos is " + i);
        }
        if (indexCol.contains(realIdx)) {
            indexValue.put(realIdx, String.valueOf(i1));
        }
    }

    @Override
    public void setInt(int i, int i1) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (!rowBuilder.setInt(realIdx, i1)) {
            throw new SQLException("set int failed. pos is " + i);
        }
        if (indexCol.contains(realIdx)) {
            indexValue.put(realIdx, String.valueOf(i1));
        }
    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (!rowBuilder.setBigInt(realIdx, l)) {
            throw new SQLException("set long failed. pos is " + i);
        }
        if (indexCol.contains(realIdx)) {
            indexValue.put(realIdx, String.valueOf(l));
        }
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        if (!rowBuilder.setFloat(getSchemaIdx(i), v)) {
            throw new SQLException("set float failed. pos is " + i);
        }
    }

    @Override
    public void setDouble(int i, double v) throws SQLException {
        if (!rowBuilder.setDouble(getSchemaIdx(i), v)) {
            throw new SQLException("set double failed. pos is " + i);
        }
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (s == null) {
            setNull(realIdx);
            if (indexCol.contains(realIdx)) {
                indexValue.put(realIdx, InsertPreparedStatementMeta.NONETOKEN);
            }
            return;
        }
        if (!rowBuilder.setString(getSchemaIdx(i), s)) {
            throw new SQLException("set string failed. pos is " + i);
        }
        if (indexCol.contains(realIdx)) {
            if (s.isEmpty()) {
                indexValue.put(realIdx, InsertPreparedStatementMeta.EMPTY_STRING);
            } else {
                indexValue.put(realIdx, s);
            }
        }
    }

    @Override
    public void setDate(int i, Date date) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (indexCol.contains(realIdx)) {
            if (date != null) {
                indexValue.put(realIdx, String.valueOf(CodecUtil.dateToDateInt(date)));
            } else {
                indexValue.put(realIdx, InsertPreparedStatementMeta.NONETOKEN);
            }
        }
        if (date == null) {
            if (!setNull(realIdx)) {
                throw new SQLException("set date failed. pos is " + i);
            }
            return;
        }
        if (!rowBuilder.setDate(realIdx, date)) {
            throw new SQLException("set date failed. pos is " + i);
        }
    }


    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        int realIdx = getSchemaIdx(i);
        if (indexCol.contains(realIdx)) {
            if (timestamp != null) {
                indexValue.put(realIdx, String.valueOf(timestamp.getTime()));
            } else {
                indexValue.put(realIdx, InsertPreparedStatementMeta.NONETOKEN);
            }
        }
        if (timestamp == null) {
            if (!setNull(realIdx)) {
                throw new SQLException("set timestamp failed. pos is " + i);
            }
            return;
        }
        if (!rowBuilder.setTimestamp(realIdx, timestamp)) {
            throw new SQLException("set timestamp failed. pos is " + i);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        rowBuilder.clear();
        indexValue.clear();
    }

    private ByteBuffer buildDimension() throws SQLException {
        int totalLen = 0;
        Map<Integer, Integer> lenMap = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : indexMap.entrySet()) {
            totalLen += 4; // encode the size of idx(int)
            totalLen += 4; // encode the value size
            int curLen = entry.getValue().size() - 1;
            for (Integer pos : entry.getValue()) {
                if (indexValue.containsKey(pos)) {
                    curLen += indexValue.get(pos).getBytes(CodecUtil.CHARSET).length;
                } else if (defaultIndexValue.containsKey(pos)) {
                    curLen += defaultIndexValue.get(pos).getBytes(CodecUtil.CHARSET).length;
                } else {
                    throw new SQLException("cannot get index value. pos is " + pos);
                }
            }
            totalLen += curLen;
            lenMap.put(entry.getKey(), curLen);
        }
        ByteBuffer dimensionValue = ByteBuffer.allocate(totalLen).order(ByteOrder.LITTLE_ENDIAN);
        for (Map.Entry<Integer, List<Integer>> entry : indexMap.entrySet()) {
            Integer indexPos = entry.getKey();
            dimensionValue.putInt(indexPos);
            dimensionValue.putInt(lenMap.get(indexPos));
            for (int i = 0; i < entry.getValue().size(); i++) {
                int pos = entry.getValue().get(i);
                if (i > 0) {
                    dimensionValue.put((byte)'|');
                }
                if (indexValue.containsKey(pos)) {
                    dimensionValue.put(indexValue.get(pos).getBytes(CodecUtil.CHARSET));
                } else {
                    dimensionValue.put(defaultIndexValue.get(pos).getBytes(CodecUtil.CHARSET));
                }
            }
        }
        return dimensionValue;
    }

    private ByteBuffer buildRow() throws SQLException {
        Map<Integer, Object> defaultValue = cache.getDefaultValue();
        if (!defaultValue.isEmpty()) {
            for (Map.Entry<Integer, Object> entry : defaultValue.entrySet()) {
                int idx = entry.getKey();
                Object val = entry.getValue();
                if (val == null) {
                    rowBuilder.setNULL(idx);
                    continue;
                }
                switch (cache.getSchema().getColumnType(idx)) {
                    case Types.BOOLEAN:
                        rowBuilder.setBool(idx, (boolean)val);
                        break;
                    case Types.SMALLINT:
                        rowBuilder.setSmallInt(idx, (short)val);
                        break;
                    case Types.INTEGER:
                        rowBuilder.setInt(idx, (int)val);
                        break;
                    case Types.BIGINT:
                        rowBuilder.setBigInt(idx, (long)val);
                        break;
                    case Types.FLOAT:
                        rowBuilder.setFloat(idx, (float)val);
                        break;
                    case Types.DOUBLE:
                        rowBuilder.setDouble(idx, (double)val);
                        break;
                    case Types.DATE:
                        rowBuilder.setDate(idx, (Date)val);
                        break;
                    case Types.TIMESTAMP:
                        rowBuilder.setTimestamp(idx, (Timestamp)val);
                        break;
                    case Types.VARCHAR:
                        rowBuilder.setString(idx, (String)val);
                        break;
                }
            }
        }
        if (!rowBuilder.build()) {
            throw new SQLException("encode row failed");
        }
        return rowBuilder.getValue();
    }

    @Override
    public boolean execute() throws SQLException {
        if (closed) {
            throw new SQLException("InsertPreparedStatement closed");
        }
        if (!batchValues.isEmpty()) {
            throw new SQLException("please use executeBatch");
        }
        ByteBuffer dimensions = buildDimension();
        ByteBuffer value = buildRow();
        Status status = new Status();
        // actually only one row
        boolean ok = router.ExecuteInsert(cache.getDatabase(), cache.getName(),
                cache.getTid(), cache.getPartitionNum(),
                dimensions.array(), dimensions.capacity(), value.array(), value.capacity(), status);
        // cleanup rows even if insert failed
        // we can't execute() again without set new row, so we must clean up here
        clearParameters();
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
        batchValues.add(new AbstractMap.SimpleImmutableEntry<>(buildDimension(), buildRow()));
        clearParameters();
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new SQLInsertMetaData(cache.getSchema(), cache.getHoleIdx());
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        setDate(i, date);
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        setTimestamp(i, timestamp);
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (closed) {
            throw new SQLException("InsertPreparedStatement closed");
        }
        int[] result = new int[batchValues.size()];
        Status status = new Status();
        for (int i = 0; i < batchValues.size(); i++) {
            AbstractMap.SimpleImmutableEntry<ByteBuffer, ByteBuffer> pair = batchValues.get(i);
            boolean ok = router.ExecuteInsert(cache.getDatabase(), cache.getName(),
                    cache.getTid(), cache.getPartitionNum(),
                    pair.getKey().array(), pair.getKey().capacity(),
                    pair.getValue().array(), pair.getValue().capacity(), status);
            if (!ok) {
                // TODO(hw): may lost log, e.g. openmldb-batch online import in yarn mode?
                logger.warn(status.ToString());
            }
            result[i] = ok ? 0 : -1;
        }
        status.delete();
        clearParameters();
        batchValues.clear();
        return result;
    }
}

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

package com._4paradigm.openmldb.sdk;

import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.common.codec.CodecMetaData;
import com._4paradigm.openmldb.sdk.impl.CallableDirectResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueryFuture implements Future<java.sql.ResultSet>{
    private static final Logger logger = LoggerFactory.getLogger(QueryFuture.class);
    com._4paradigm.openmldb.QueryFuture queryFuture;
    Schema schema;
    CodecMetaData metaData;

    public QueryFuture(com._4paradigm.openmldb.QueryFuture queryFuture, Schema schema, CodecMetaData metaData) {
        this.queryFuture = queryFuture;
        this.schema = schema;
        this.metaData = metaData;
    }

    @Override
    @Deprecated
    public boolean cancel(boolean b) {
        return false;
    }

    @Override
    @Deprecated
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        if (queryFuture != null) {
            return queryFuture.IsDone();
        }
        return true;
    }

    @Override
    public java.sql.ResultSet get() throws InterruptedException, ExecutionException {
        if (queryFuture == null) {
            throw new ExecutionException(new SqlException("queryFuture is null"));
        }
        Status status = new Status();
        com._4paradigm.openmldb.ResultSet resultSet = queryFuture.GetResultSet(status);
        if (status.getCode() != 0 || resultSet == null) {
            String msg = status.ToString();
            status.delete();
            if (resultSet != null) {
                resultSet.delete();
            }
            logger.error("call procedure failed: {}", msg);
            throw new ExecutionException(new SqlException("call procedure failed: " + msg));
        }
        status.delete();
        int totalRows = resultSet.Size();
        int dataLength = resultSet.GetDataLength();
        ByteBuffer dataBuf = ByteBuffer.allocateDirect(dataLength).order(ByteOrder.LITTLE_ENDIAN);
        resultSet.CopyTo(dataBuf);
        resultSet.delete();
        queryFuture.delete();
        queryFuture = null;
        return new CallableDirectResultSet(dataBuf, totalRows, schema, metaData);
    }

    /**
     *
     * @param l  current timeout set by executeQeuryAsyn, so the param is invalid
     * @param timeUnit the time unit of the timeout, which is also invalid.
     * @return the result of the query from the database
     * @throws InterruptedException throws when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted, either before or during the activity.
     * @throws ExecutionException throws when attempting to retrieve the result of a task that aborted by throwing an exception.
     * @throws TimeoutException throws when a blocking operation times out.
     */
    @Override
    @Deprecated
    public java.sql.ResultSet get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }
}

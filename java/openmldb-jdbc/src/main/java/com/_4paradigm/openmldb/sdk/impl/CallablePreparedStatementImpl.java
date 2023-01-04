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

import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.QueryFuture;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class CallablePreparedStatementImpl extends CallablePreparedStatement {

    public CallablePreparedStatementImpl(String db, String spName, SQLRouter router) throws SQLException {
        super(db, spName, router);
    }

    @Override
    public SQLResultSet executeQuery() throws SQLException {
        checkClosed();
        checkExecutorClosed();
        dataBuild();
        Status status = new Status();
        com._4paradigm.openmldb.ResultSet resultSet = router.CallProcedure(db, spName, currentRow, status);
        if (status.getCode() != 0 || resultSet == null) {
            String msg = status.ToString();
            status.delete();
            if (resultSet != null) {
                resultSet.delete();
            }
            throw new SQLException("call procedure fail, msg: " + msg);
        }
        status.delete();
        SQLResultSet rs = new SQLResultSet(resultSet);
        if (closeOnComplete) {
            closed = true;
        }
        return rs;
    }

    @Override
    public QueryFuture executeQueryAsync(long timeOut, TimeUnit unit) throws SQLException {
        checkClosed();
        checkExecutorClosed();
        dataBuild();
        Status status = new Status();
        com._4paradigm.openmldb.QueryFuture queryFuture = router.CallProcedure(db, spName, unit.toMillis(timeOut), currentRow, status);
        if (status.getCode() != 0 || queryFuture == null) {
            String msg = status.ToString();
            status.delete();
            if (queryFuture != null) {
                queryFuture.delete();
            }
            throw new SQLException("call procedure fail, msg: " + msg);
        }
        status.delete();
        return new QueryFuture(queryFuture);
    }

}

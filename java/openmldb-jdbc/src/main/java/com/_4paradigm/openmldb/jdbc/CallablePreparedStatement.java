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

package com._4paradigm.openmldb.jdbc;

import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.common.codec.FlexibleRowBuilder;
import com._4paradigm.openmldb.sdk.QueryFuture;
import com._4paradigm.openmldb.sdk.impl.Deployment;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public abstract class CallablePreparedStatement extends PreparedStatement {
    protected Deployment deployment;
    protected FlexibleRowBuilder rowBuilder;
    protected String db;
    protected String deploymentName;

    public CallablePreparedStatement(Deployment deployment, SQLRouter router) throws SQLException {
        if (router == null) throw new SQLException("router is null");
        this.router = router;
        this.deployment = deployment;
        db = deployment.getDatabase();
        deploymentName = deployment.getName();
    }

    public QueryFuture executeQueryAsync(long timeOut, TimeUnit unit) throws SQLException {
        throw new SQLException("current do not support this method");
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new SQLResultSetMetaData(deployment.getInputSchema());
    }
}

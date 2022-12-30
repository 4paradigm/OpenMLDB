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
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.sdk.QueryFuture;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class CallablePreparedStatement extends RequestPreparedStatement {
    protected String spName;
    private com._4paradigm.openmldb.ProcedureInfo procedureInfo;

    public CallablePreparedStatement(String db, String spName, SQLRouter router) throws SQLException {
        if (db == null) throw new SQLException("db is null");
        if (router == null) throw new SQLException("router is null");
        if (spName == null) throw new SQLException("spName is null");

        this.db = db;
        this.router = router;
        this.spName = spName;

        Status status = new Status();
        procedureInfo = router.ShowProcedure(db, spName, status);
        if (procedureInfo == null || status.getCode() != 0) {
            String msg = status.ToString();
            status.delete();
            throw new SQLException("show procedure failed, msg: " + msg);
        }
        this.currentSql = procedureInfo.GetSql();
        this.currentRow = router.GetRequestRow(db, procedureInfo.GetSql(), status);
        if (status.getCode() != 0 || this.currentRow == null) {
            String msg = status.ToString();
            status.delete();
            throw new SQLException("getRequestRow failed!, msg: " + msg);
        }
        status.delete();
        this.currentSchema = procedureInfo.GetInputSchema();
        if (this.currentSchema == null) {
            throw new SQLException("inputSchema is null");
        }
        int cnt = this.currentSchema.GetColumnCnt();
        this.currentDatas = new ArrayList<>(cnt);
        this.hasSet = new ArrayList<>(cnt);
        for (int i = 0; i < cnt; i++) {
            this.hasSet.add(false);
            currentDatas.add(null);
        }
    }

    @Override
    public void close() throws SQLException {
        super.close();
        this.spName = null;
        if (this.procedureInfo != null) {
            procedureInfo.delete();
            procedureInfo = null;
        }
    }

    public QueryFuture executeQueryAsync(long timeOut, TimeUnit unit) throws SQLException {
        throw new SQLException("current do not support this method");
    }
}

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
import com._4paradigm.openmldb.jdbc.RequestPreparedStatement;

import java.sql.SQLException;
import java.util.ArrayList;

public class RequestPreparedStatementImpl extends RequestPreparedStatement {

    public RequestPreparedStatementImpl(String db, String sql, SQLRouter router) throws SQLException {
        if (db == null) throw new SQLException("db is null");
        if (router == null) throw new SQLException("router is null");
        if (sql == null) throw new SQLException("spName is null");
        this.db = db;
        this.currentSql = sql;
        this.router = router;
        Status status = new Status();
        this.currentRow = router.GetRequestRow(db, sql, status);
        if (status.getCode() != 0 || this.currentRow == null) {
            String msg = status.ToString();
            status.delete();
            if (currentRow != null) {
                currentRow.delete();
                currentRow = null;
            }
            throw new SQLException("get GetRequestRow fail " + msg + " in construction preparedstatement");
        }
        status.delete();
        this.currentSchema = this.currentRow.GetSchema();
        int cnt = this.currentSchema.GetColumnCnt();
        this.currentDatas = new ArrayList<>(cnt);
        this.hasSet = new ArrayList<>(cnt);
        for (int i = 0; i < cnt; i++) {
            this.hasSet.add(false);
            currentDatas.add(null);
        }
    }
}

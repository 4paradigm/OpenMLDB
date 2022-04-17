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

import java.sql.SQLException;
import java.util.TreeMap;

import com._4paradigm.openmldb.DataType;
import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.jdbc.PreparedStatement;

public class PreparedStatementImpl extends PreparedStatement  {

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
}

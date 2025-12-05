/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.hybridse.sdk;

import com._4paradigm.hybridse.base.BaseStatus;
import com._4paradigm.hybridse.type.TypeOuterClass;
import com._4paradigm.hybridse.vm.CompileInfo;
import com._4paradigm.hybridse.vm.Engine;
import com._4paradigm.hybridse.vm.EngineOptions;
import com._4paradigm.hybridse.vm.PhysicalOpNode;
import com._4paradigm.hybridse.vm.RequestRunSession;
import com._4paradigm.hybridse.vm.SimpleCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQL Engine in request mode.
 */
public class RequestEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RequestEngine.class);

    private SimpleCatalog catalog;
    private EngineOptions options;
    private Engine engine;
    private RequestRunSession session;
    private CompileInfo compileInfo;
    private PhysicalOpNode plan;


    /**
     * Construct RequestEngine with given sql and database.
     *
     * @param sql query the sql string
     * @param database query on the database
     * @throws UnsupportedHybridSeException throw when query unsupported or has syntax error
     */
    public RequestEngine(String sql, TypeOuterClass.Database database) throws UnsupportedHybridSeException {
        options = new EngineOptions();
        options.SetKeepIr(true);
        options.SetCompileOnly(true);
        catalog = new SimpleCatalog();
        session = new RequestRunSession();
        catalog.AddDatabase(database);
        engine = new Engine(catalog, options);

        BaseStatus status = new BaseStatus();
        boolean ok = engine.Get(sql, database.getName(), session, status);
        if (!(ok && status.getMsg().equals("ok"))) {
            throw new UnsupportedHybridSeException("SQL parse error: " + status.getMsg());
        }
        status.delete();
        compileInfo = session.GetCompileInfo();
        plan = compileInfo.GetPhysicalPlan();
    }

    /**
     * Get physical plan.
     */
    public PhysicalOpNode getPlan() {
        return plan;
    }

    /**
     * Close the request engine.
     */
    @Override
    public synchronized void close() {
        engine.delete();
        engine = null;

        compileInfo.delete();
        compileInfo = null;

        options.delete();
        options = null;

        plan.delete();
        plan = null;

        session.delete();
        session = null;

        catalog.delete();
        catalog = null;
    }
}

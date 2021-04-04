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

package com._4paradigm.hybridse.sdk;

import com._4paradigm.hybridse.base.BaseStatus;
import com._4paradigm.hybridse.type.TypeOuterClass;
import com._4paradigm.hybridse.vm.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;


public class SQLEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SQLEngine.class);

    private SimpleCatalog catalog;
    private EngineOptions options;
    private Engine engine;
    private BatchRunSession session;
    private CompileInfo compileInfo;
    private PhysicalOpNode plan;

    public SQLEngine(String sql, TypeOuterClass.Database database) throws UnsupportedHybridSeException {
        // Create the default engine options
        this.initilize(sql, database, createDefaultEngineOptions());
    }
    public SQLEngine(String sql, TypeOuterClass.Database database, EngineOptions engineOptions) throws UnsupportedHybridSeException {
        this.initilize(sql, database, engineOptions);
    }

    public static EngineOptions createDefaultEngineOptions() {
        EngineOptions engineOptions = new EngineOptions();
        engineOptions.set_keep_ir(true);
        engineOptions.set_compile_only(true);
        engineOptions.set_performance_sensitive(false);
        return engineOptions;
    }

    public void initilize(String sql, TypeOuterClass.Database database, EngineOptions engineOptions) throws UnsupportedHybridSeException {
        options = engineOptions;
        catalog = new SimpleCatalog();
        session = new BatchRunSession();
        catalog.AddDatabase(database);
        engine = new Engine(catalog, options);

        BaseStatus status = new BaseStatus();
        boolean ok = engine.Get(sql, database.getName(), session, status);
        if (! (ok && status.getMsg().equals("ok"))) {
            throw new UnsupportedHybridSeException("SQL parse error: " + status.getMsg() + "\n" + status.getTrace());
        }
        status.delete();
        compileInfo = session.GetCompileInfo();
        plan = compileInfo.GetPhysicalPlan();
    }

    public PhysicalOpNode getPlan() {
        return plan;
    }

    public ByteBuffer getIRBuffer() {
        long size = compileInfo.GetIRSize();
        ByteBuffer buffer = ByteBuffer.allocateDirect(Long.valueOf(size).intValue());
        compileInfo.GetIRBuffer(buffer);
        logger.info("Dumped module size: {}", size);
        return buffer;
    }

    @Override
    synchronized public void close() throws Exception {
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

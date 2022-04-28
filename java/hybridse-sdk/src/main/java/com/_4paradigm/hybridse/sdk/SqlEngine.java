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
import com._4paradigm.hybridse.vm.BatchRunSession;
import com._4paradigm.hybridse.vm.CompileInfo;
import com._4paradigm.hybridse.vm.Engine;
import com._4paradigm.hybridse.vm.EngineOptions;
import com._4paradigm.hybridse.vm.PhysicalOpNode;
import com._4paradigm.hybridse.vm.SimpleCatalog;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of HybridSE SQL simple engine that compiled queries with given sql and database.
 */
public class SqlEngine implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SqlEngine.class);

    private SimpleCatalog catalog;
    private EngineOptions options;
    private Engine engine;
    private BatchRunSession session;
    private CompileInfo compileInfo;
    private PhysicalOpNode plan;


    public SqlEngine(List<TypeOuterClass.Database> databases, EngineOptions engineOptions) {
        // TODO(tobe): This is only used for SparkPlanner
        initilizeEngine(databases, engineOptions);
    }

    /**
     * Construct SQL engine for specific sql and database.
     *
     * @throws UnsupportedHybridSeException throws exception when fail to compile queries
     */
    public SqlEngine(String sql, TypeOuterClass.Database database) throws UnsupportedHybridSeException {
        // Create the default engine options
        this.initilize(sql, Arrays.<TypeOuterClass.Database>asList(database), createDefaultEngineOptions(),
                database.getName());
    }

    /**
     * Construct SQL engine for specific sql and databases.
     *
     * @throws UnsupportedHybridSeException throws exception when fail to compile queries
     */
    public SqlEngine(String sql, List<TypeOuterClass.Database> databases, String defaultDbname)
            throws UnsupportedHybridSeException {
        // Create the default engine options
        this.initilize(sql, databases, createDefaultEngineOptions(), defaultDbname);
    }

    /**
     * Construct SQL engine for specific sql, database and EngineOptions.
     *
     * @throws UnsupportedHybridSeException throws exception when fail to compile queries
     */
    public SqlEngine(String sql, TypeOuterClass.Database database, EngineOptions engineOptions)
            throws UnsupportedHybridSeException {
        this.initilize(sql, Arrays.<TypeOuterClass.Database>asList(database), engineOptions, database.getName());
    }

    /**
     * Construct SQL engine for specific sql, databases and EngineOptions.
     *
     * @throws UnsupportedHybridSeException throws exception when fail to compile queries
     */
    public SqlEngine(String sql, List<TypeOuterClass.Database> databases, EngineOptions engineOptions,
                     String defaultDbName) throws UnsupportedHybridSeException {
        this.initilize(sql, databases, engineOptions, defaultDbName);
    }

    /**
     * Create default engine option.
     * 
     * <p>- Enable store ir results into SQL context
     * - Only compile SQL
     * - Disable performance sensitive mode.
     *
     * @return Return default engine option
     */
    public static EngineOptions createDefaultEngineOptions() {
        EngineOptions engineOptions = new EngineOptions();
        engineOptions.SetKeepIr(true);
        engineOptions.SetCompileOnly(true);
        return engineOptions;
    }

    /**
     * Initialize engine with given sql, database and specified engine options.
     *
     * @param sql query sql string
     * @param databases query on the databases
     * @param engineOptions query engine options
     * @param defaultDbName default database name
     * @throws UnsupportedHybridSeException throw when query unsupported or has syntax error
     */
    public void initilize(String sql, List<TypeOuterClass.Database> databases, EngineOptions engineOptions,
                          String defaultDbName)
            throws UnsupportedHybridSeException {
        initilizeEngine(databases, engineOptions);
        compileSql(sql, defaultDbName);
    }

    public void initilizeEngine(List<TypeOuterClass.Database> databases, EngineOptions engineOptions) {
        options = engineOptions;
        catalog = new SimpleCatalog();
        session = new BatchRunSession();
        for (TypeOuterClass.Database database: databases) {
            catalog.AddDatabase(database);
        }
        engine = new Engine(catalog, options);
    }

    public void compileSql(String sql, String defaultDbName) throws UnsupportedHybridSeException {
        BaseStatus status = new BaseStatus();
        boolean ok = engine.Get(sql, Objects.isNull(defaultDbName) ? "" : defaultDbName, session, status);
        if (!(ok && status.getMsg().equals("ok"))) {
            throw new UnsupportedHybridSeException("SQL parse error: " + status.GetMsg() + "\n" + status.GetTraces());
        }
        status.delete();
        this.compileInfo = session.GetCompileInfo();
        this.plan = compileInfo.GetPhysicalPlan();
    }

    /**
     * Return physical plan.
     */
    public PhysicalOpNode getPlan() {
        return plan;
    }

    /**
     * Return compile IR result as ByteBuffer.
     */
    public ByteBuffer getIrBuffer() {
        long size = compileInfo.GetIRSize();
        ByteBuffer buffer = ByteBuffer.allocateDirect(Long.valueOf(size).intValue());
        compileInfo.GetIRBuffer(buffer);
        logger.info("Dumped module size: {}", size);
        return buffer;
    }

    public Engine getEngine() {
        return engine;
    }

    @Override
     public synchronized void close() throws Exception {
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

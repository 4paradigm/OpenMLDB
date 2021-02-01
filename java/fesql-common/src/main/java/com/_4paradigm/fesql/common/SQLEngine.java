package com._4paradigm.fesql.common;

import com._4paradigm.fesql.base.BaseStatus;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.*;
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

    public SQLEngine(String sql, TypeOuterClass.Database database) throws UnsupportedFesqlException {
        // Create the default engine options
        this.initilize(sql, database, createDefaultEngineOptions());
    }
    public SQLEngine(String sql, TypeOuterClass.Database database, EngineOptions engineOptions) throws UnsupportedFesqlException {
        this.initilize(sql, database, engineOptions);
    }

    public static EngineOptions createDefaultEngineOptions() {
        EngineOptions engineOptions = new EngineOptions();
        engineOptions.set_keep_ir(true);
        engineOptions.set_compile_only(true);
        engineOptions.set_performance_sensitive(false);
        return engineOptions;
    }

    public void initilize(String sql, TypeOuterClass.Database database, EngineOptions engineOptions) throws UnsupportedFesqlException {
        options = engineOptions;
        catalog = new SimpleCatalog();
        session = new BatchRunSession();
        catalog.AddDatabase(database);
        engine = new Engine(catalog, options);

        BaseStatus status = new BaseStatus();
        boolean ok = engine.Get(sql, database.getName(), session, status);
        if (! (ok && status.getMsg().equals("ok"))) {
            throw new UnsupportedFesqlException("SQL parse error: " + status.getMsg());
        }
        status.delete();

        compileInfo = session.GetCompileInfo();
        plan = session.GetPhysicalPlan();
    }

    public PhysicalOpNode getPlan() {
        return plan;
    }

    public ByteBuffer getIRBuffer() {
        long size = compileInfo.get_ir_size();
        ByteBuffer buffer = ByteBuffer.allocateDirect(Long.valueOf(size).intValue());
        compileInfo.get_ir_buffer(buffer);
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

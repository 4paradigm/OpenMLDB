package com._4paradigm.fesql.common;

import com._4paradigm.fesql.FeSqlLibrary;
import com._4paradigm.fesql.vm.Engine;
import com._4paradigm.fesql.vm.FeSQLJITWrapper;
import com._4paradigm.fesql.vm.JITOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;


public class JITManager {

    static private Logger logger = LoggerFactory.getLogger(JITManager.class);

    static {
        FeSqlLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    // One jit currently only take one llvm module, since symbol may duplicate
    static private Map<String, FeSQLJITWrapper> jits = new HashMap<>();
    static private Set<String> initializedModuleTags = new HashSet<>();

    synchronized static public FeSQLJITWrapper getJIT(String tag) {
        if (! jits.containsKey(tag)) {
            FeSQLJITWrapper jit = FeSQLJITWrapper.Create(getJITOptions());
            if (jit == null) {
                throw new RuntimeException("Fail to create native jit");
            }
            jit.Init();
            FeSQLJITWrapper.InitJITSymbols(jit);
            jits.put(tag, jit);
        }
        return jits.get(tag);
    }

    static private JITOptions getJITOptions() {
        JITOptions options = new JITOptions();
        try (InputStream input = JITManager.class.getClassLoader().getResourceAsStream(
                "jit.properties")) {
            Properties prop = new Properties(System.getProperties());
            if (input == null) {
                return options;
            }
            prop.load(input);
            String enableMCJIT = prop.getProperty("fesql.jit.enable_mcjit");
            if (enableMCJIT != null && enableMCJIT.toLowerCase().equals("true")) {
                logger.info("Try enable llvm legacy mcjit support");
                options.set_enable_mcjit(true);
            }
            String enableVTune = prop.getProperty("fesql.jit.enable_vtune");
            if (enableVTune != null && enableVTune.toLowerCase().equals("true")) {
                logger.info("Try enable intel jit events support");
                options.set_enable_vtune(true);
            }
            String enablePerf = prop.getProperty("fesql.jit.enable_perf");
            if (enablePerf != null && enablePerf.toLowerCase().equals("true")) {
                logger.info("Try enable perf jit events support");
                options.set_enable_perf(true);
            }
            String enableGdb = prop.getProperty("fesql.jit.enable_gdb");
            if (enableGdb != null && enableGdb.toLowerCase().equals("true")) {
                logger.info("Try enable gdb jit events support");
                options.set_enable_gdb(true);
            }
        } catch (IOException ex) {
            logger.debug("Can not find jit.properties", ex);
        }
        return options;
    }

    synchronized static private boolean hasModule(String tag) {
        return initializedModuleTags.contains(tag);
    }

    synchronized static private void initModule(String tag, ByteBuffer moduleBuffer) {
        FeSQLJITWrapper jit = getJIT(tag);
        if (! moduleBuffer.isDirect()) {
            throw new RuntimeException("JIT must use direct buffer");
        }
        if (!jit.AddModuleFromBuffer(moduleBuffer)) {
            throw new RuntimeException("Fail to initialize native module");
        }
        initializedModuleTags.add(tag);
    }

    synchronized static public void initJITModule(String tag, ByteBuffer moduleBuffer) {

        // ensure worker native
        FeSqlLibrary.initCore();

        // ensure worker side module
        if (!JITManager.hasModule(tag)) {
            JITManager.initModule(tag, moduleBuffer);
            logger.info("Init jit module with tag:\n{}",  tag);
        }
    }
}

package com._4paradigm.fesql.offline;

import com._4paradigm.fesql.vm.FeSQLJITWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Byte;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JITManager {

    static private Logger logger = LoggerFactory.getLogger(JITManager.class);

    // One jit currently only take one llvm module, since symbol may duplicate
    static private Map<String, FeSQLJITWrapper> jits = new HashMap<>();
    static private Set<String> initializedModuleTags = new HashSet<>();

    synchronized static public FeSQLJITWrapper getJIT(String tag) {
        if (! jits.containsKey(tag)) {
            FeSQLJITWrapper jit = new FeSQLJITWrapper();
            jit.Init();
            jits.put(tag, jit);
        }
        return jits.get(tag);
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
        FeSqlLibrary.init();

        // ensure worker side module
        if (!JITManager.hasModule(tag)) {
            JITManager.initModule(tag, moduleBuffer);
            logger.info("Init jit module with tag:\n" + tag);
        }
    }
}

package com._4paradigm.fesql.offline;

import com._4paradigm.fesql.vm.FeSQLJITWrapper;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class JITManager {

    // currently we use singleton instance of jit
    static private FeSQLJITWrapper jit;

    static private Set<String> initializedModuleTags = new HashSet<>();

    synchronized static public FeSQLJITWrapper getJIT() {
        if (jit == null) {
            jit = new FeSQLJITWrapper();
            jit.Init();
        }
        return jit;
    }

    synchronized static public boolean hasModule(String tag) {
        return initializedModuleTags.contains(tag);
    }

    synchronized static public void initModule(String tag, ByteBuffer moduleBuffer) {
        FeSQLJITWrapper jit = getJIT();
        if (! moduleBuffer.isDirect()) {
            throw new RuntimeException("JIT must use direct buffer");
        }
        if (! initializedModuleTags.contains(tag)) {
            jit.AddModuleFromBuffer(moduleBuffer);
            initializedModuleTags.add(tag);
        }
    }
}

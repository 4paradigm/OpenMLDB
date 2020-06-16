package com._4paradigm.fesql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeSqlLibrary {
    private static final Logger logger = LoggerFactory.getLogger(FeSqlLibrary.class.getName());

    static private final String FESQL_JSDK_CORE_NAME = "fesql_jsdk_core";
    static private final String FESQL_JSDK_COMPLETE_NAME = "fesql_jsdk_complete";

    static private boolean isLoadCompleteLib = false;
    static private boolean initialized = false;

    static synchronized public void initCore() {
        if (initialized) {
            if (isLoadCompleteLib) {
                logger.warn("fesql_jsdk_complete is load before initCore()");
            }
            return;
        }
        LibraryLoader.loadLibrary(FESQL_JSDK_CORE_NAME);
        isLoadCompleteLib = false;
        initialized = true;
    }

    static synchronized public void initComplete() {
        if (initialized) {
            if (!isLoadCompleteLib) {
                throw new RuntimeException("fesql_jsdk_core is load before initComplete()");
            }
            return;
        }
        LibraryLoader.loadLibrary(FESQL_JSDK_COMPLETE_NAME);
        isLoadCompleteLib = true;
        initialized = true;
    }
}

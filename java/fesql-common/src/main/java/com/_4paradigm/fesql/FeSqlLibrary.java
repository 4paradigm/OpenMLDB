package com._4paradigm.fesql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeSqlLibrary {
    private static final Logger logger = LoggerFactory.getLogger(FeSqlLibrary.class.getName());

    static private final String FESQL_LIBRARY_NAME = "fesql_jsdk";

    static private boolean initialized = false;

    static synchronized public void init() {
        if (initialized) {
            return;
        }
        LibraryLoader.loadLibrary(FESQL_LIBRARY_NAME);
        initialized = true;
    }
}

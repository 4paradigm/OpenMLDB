package com._4paradigm.fesql;

public class FeSqlLibrary {

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

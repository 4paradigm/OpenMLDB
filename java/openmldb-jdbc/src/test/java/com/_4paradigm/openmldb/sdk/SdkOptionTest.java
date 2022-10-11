package com._4paradigm.openmldb.sdk;

import org.testng.Assert;
import org.testng.annotations.Test;

import com._4paradigm.openmldb.SQLRouterOptions;
import com._4paradigm.openmldb.StandaloneOptions;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

public class SdkOptionTest {
    @Test
    void testGetSet() {
        // use native class must init lib first
        SqlClusterExecutor.initJavaSdkLibrary(null);
        SdkOption option = new SdkOption();
        try {
            SQLRouterOptions co = option.buildSQLRouterOptions();
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("empty zk"));
        }
        try {
            option.setClusterMode(false);
            StandaloneOptions co = option.buildStandaloneOptions();
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("empty host"));
        }

    }
}

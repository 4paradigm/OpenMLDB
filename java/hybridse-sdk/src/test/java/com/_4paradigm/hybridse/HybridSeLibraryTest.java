package com._4paradigm.hybridse;

import org.testng.Assert;
import org.testng.annotations.Test;

public class HybridSeLibraryTest {
    @Test
    public void initCoreTest() {
        Assert.assertFalse(HybridSeLibrary.isInitialized());
        HybridSeLibrary.initCore();
        Assert.assertTrue(HybridSeLibrary.isInitialized());
    }
}

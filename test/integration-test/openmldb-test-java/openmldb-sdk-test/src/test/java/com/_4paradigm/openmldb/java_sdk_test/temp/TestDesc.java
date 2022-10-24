package com._4paradigm.openmldb.java_sdk_test.temp;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBTest;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import org.testng.annotations.Test;

public class TestDesc extends OpenMLDBTest {
    @Test
    public void test(){
        SDKUtil.desc(executor,"test_zw","desc auto_xxNxKHFO;");
        
    }
}

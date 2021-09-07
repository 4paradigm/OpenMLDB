package com._4paradigm.openmldb.http_test.v230;


import com._4paradigm.openmldb.http_test.common.BaseTest;
import com._4paradigm.openmldb.http_test.executor.RestfulExecutor;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import org.testng.annotations.Test;

public class TestInsert extends BaseTest {
    @Yaml(filePaths = "/restful/v230/test_insert.yaml")
    @Test(dataProvider = "getCase")
    public void testInsert(RestfulCase restfulCase){
        new RestfulExecutor(executor, restfulCase).run();
    }
}

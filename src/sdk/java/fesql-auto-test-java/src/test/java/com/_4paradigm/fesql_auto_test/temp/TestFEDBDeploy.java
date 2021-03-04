package com._4paradigm.fesql_auto_test.temp;

import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.util.FEDBDeploy;
import org.testng.annotations.Test;

public class TestFEDBDeploy {
    @Test
    public void test1(){
        FEDBDeploy deploy = new FEDBDeploy("2021-02-06");
        deploy.deployFEDB(2,3);
    }
    @Test
    public void test2(){
        FEDBDeploy deploy = new FEDBDeploy("fedb");
        FEDBInfo fedbInfo = deploy.deployFEDB(2, 3);
        System.out.println(fedbInfo);
    }
}

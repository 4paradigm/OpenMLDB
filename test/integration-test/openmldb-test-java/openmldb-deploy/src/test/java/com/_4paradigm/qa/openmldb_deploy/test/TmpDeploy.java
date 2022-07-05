package com._4paradigm.qa.openmldb_deploy.test;

import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class TmpDeploy {
    @Test
    @Parameters({"version","openMLDBPath"})
    public void testTmp(@Optional("tmp") String version,@Optional("") String openMLDBPath){
        OpenMLDBDeploy deploy = new OpenMLDBDeploy(version);
        deploy.setOpenMLDBPath(openMLDBPath);
        deploy.setCluster(true);
        deploy.setSparkMaster("local");
        OpenMLDBInfo openMLDBInfo = deploy.deployFEDB(2, 3);
        System.out.println(openMLDBInfo);
    }
}

package com._4paradigm.qa.openmldb_deploy.test;

import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.qa.openmldb_deploy.common.OpenMLDBDeploy;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class TmpDeployStandalone {
    @Test
    @Parameters({"openMLDBPath"})
    public void testTmp(@Optional("") String openMLDBPath){
        OpenMLDBDeploy deploy = new OpenMLDBDeploy("standalone");
        deploy.setOpenMLDBPath(openMLDBPath);
        OpenMLDBInfo openMLDBInfo = deploy.deployStandalone();
        System.out.println(openMLDBInfo);
    }
}

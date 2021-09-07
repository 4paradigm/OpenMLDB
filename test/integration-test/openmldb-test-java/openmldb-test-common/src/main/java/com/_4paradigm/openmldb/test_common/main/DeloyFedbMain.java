package com._4paradigm.openmldb.test_common.main;


import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.util.FEDBDeploy;

public class DeloyFedbMain {
    public static void main(String[] args) {
        String version = args[0];
        FEDBDeploy deploy = new FEDBDeploy(version);
        FEDBInfo fedbInfo = deploy.deployFEDB(2,3);
        System.out.println(fedbInfo);
    }
}

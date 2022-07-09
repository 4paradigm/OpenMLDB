package com._4paradigm.openmldb.devops_test.tmp;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.test_common.command.OpenMLDBComamndFacade;
import com._4paradigm.openmldb.test_common.openmldb.NsClient;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import org.testng.annotations.Test;

public class TestSDKClient extends ClusterTest {

    @Test
    public void testComponents(){
//        SDKClient sdkClient = SDKClient.of(executor);
//        boolean b= sdkClient.checkComponentStatus("127.0.0.1:30001","online");
//        System.out.println("b = " + b);
//        NsClient nsClient = NsClient.of(OpenMLDBGlobalVar.mainInfo);
//        boolean flag = nsClient.checkOPStatusDone("test_devops4",null);

        OpenMLDBComamndFacade.sql(OpenMLDBGlobalVar.mainInfo,"test_devops","select * from test_ssd;");

    }
}

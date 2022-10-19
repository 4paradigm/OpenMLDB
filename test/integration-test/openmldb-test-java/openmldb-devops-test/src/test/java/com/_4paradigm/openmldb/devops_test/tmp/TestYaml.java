package com._4paradigm.openmldb.devops_test.tmp;

import com._4paradigm.openmldb.test_common.provider.YamlUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBDeployType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

public class TestYaml {
    @Test
    public void testWriteYaml(){
        OpenMLDBInfo openMLDBInfo = new OpenMLDBInfo();
        openMLDBInfo.setDeployType(OpenMLDBDeployType.CLUSTER);
        openMLDBInfo.setNsNum(2);
        openMLDBInfo.setTabletNum(3);
        openMLDBInfo.setBasePath("/home/zhaowei01/openmldb-auto-test/tmp");
        openMLDBInfo.setZk_cluster("172.24.4.55:30000");
        openMLDBInfo.setZk_root_path("/openmldb");
        openMLDBInfo.setNsEndpoints(Lists.newArrayList("172.24.4.55:30004", "172.24.4.55:30005"));
        openMLDBInfo.setNsNames(Lists.newArrayList());
        openMLDBInfo.setTabletEndpoints(Lists.newArrayList("172.24.4.55:30001", "172.24.4.55:30002", "172.24.4.55:30003"));
        openMLDBInfo.setTabletNames(Lists.newArrayList());
        openMLDBInfo.setApiServerEndpoints(Lists.newArrayList("172.24.4.55:30006"));
        openMLDBInfo.setApiServerNames(Lists.newArrayList());
        openMLDBInfo.setTaskManagerEndpoints(Lists.newArrayList("172.24.4.55:30007"));
        openMLDBInfo.setOpenMLDBPath("/home/zhaowei01/openmldb-auto-test/tmp/openmldb-ns-1/bin/openmldb");

        YamlUtil.writeYamlFile(openMLDBInfo,"out/test.yaml");
    }
    @Test
    public void testLoadYaml(){
        OpenMLDBInfo openMLDBInfo = YamlUtil.getObject("out/test.yaml", OpenMLDBInfo.class);
        System.out.println(openMLDBInfo);
    }
}

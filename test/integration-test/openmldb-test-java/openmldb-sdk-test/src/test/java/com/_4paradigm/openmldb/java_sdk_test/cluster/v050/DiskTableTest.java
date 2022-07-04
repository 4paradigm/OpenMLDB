package com._4paradigm.openmldb.java_sdk_test.cluster.v050;

import com._4paradigm.openmldb.java_sdk_test.common.FedbTest;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class DiskTableTest extends FedbTest {

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/disk_table/disk_table.yaml")
    @Story("Disk-Table")
    public void testDiskTable(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kRequest).run();
    }

    //all pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/disk_table/disk_table.yaml")
    @Story("Disk-Table")
    public void testDiskTable2(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }


    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/disk_table/disk_table.yaml")
    @Story("Disk-Table")
    public void testDiskTable3(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kClusterCLI).run();
    }
}

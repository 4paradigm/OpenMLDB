package com._4paradigm.openmldb.taskmanager;

import com._4paradigm.openmldb.taskmanager.client.TaskManagerClient;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


public abstract class TaskManagerClientTest {
    @Ignore
    @Test
    public void TestManagerClient() throws Exception{
        TaskManagerClient client = new TaskManagerClient("127.0.0.1:9995");
        client.printJobs();
        client.showJobs();
        client.importOnlineData("load data infile 'file:///tmp/test.csv' " +
                "into table test_taskmanager.t1 options(format='csv', foo='bar', header=false, mode='append');");
        client.showJobs();
        client.printJobs();
        client.showBatchVersion();
        client.stop();
    }
}

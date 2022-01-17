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
        client.dropOfflineTable("db_name","table_name");
        client.importOnlineData("load data infile 'file:///tmp/test.csv' " +
                "into table test_taskmanager.t1 options(format='csv', foo='bar', header=false, mode='append');");
        client.showJobs();
        client.printJobs();
        client.importOfflineData("load data infile 'file:///tmp/test.csv' into table test_taskmanager.t1 options(format='csv', foo='bar', header=false, mode='overwrite');");
        client.exportOfflineData("select * from test_taskmanager.t1 into outfile 'file:///tmp/test_csv2' options(format='csv', foo='bar', mode='overwrite');");
        client.showBatchVersion();
        client.stop();
    }
}

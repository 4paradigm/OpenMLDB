package com._4paradigm.openmldb.taskmanager;

import com._4paradigm.openmldb.taskmanager.client.TaskManagerClient;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


public abstract class TaskManagerClient_Test {
    @Ignore
    @Test
    public  void testTaskManagerClient() throws Exception{
        TaskManagerClient client = new TaskManagerClient("127.0.0.1:9995");
        client.showJobs();
//        client.stopJob(21);
        client.showBatchVersion();
        client.showOneJob(21);
        client.importOnlineData("show tables;");
        client.importOnlineData("show tables;","1");
//

    }

}

package com._4paradigm.openmldb.taskmanager;

import com._4paradigm.openmldb.taskmanager.client.TaskManagerClient;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


public abstract class TaskManagerClientTest {
    @Ignore
    @Test
    public void TestManagerClient() throws Exception{
        TaskManagerClient client = new TaskManagerClient("127.0.0.1:9995"); //要与taskmanager.properties里的配置一致
//        client.stopJob(0);传入job id
        client.printJobs();
        client.showJobs();
        client.importOnlineData("load data infile '/Users/4paradigm/IdeaProjects/OpenMLDB/java/openmldb-taskmanager/src/test/java/com/_4paradigm/openmldb/taskmanager/test.csv' " +
                "into table test_taskmanager.t3 options(format='csv', foo='bar', header=false, mode='append');");//插入需要大概半分钟左右的时间，注意最好不要在t1表上插入，否则会失败。
        client.showJobs();
        client.printJobs();
        System.out.println(client.showBatchVersion());;
    }
}

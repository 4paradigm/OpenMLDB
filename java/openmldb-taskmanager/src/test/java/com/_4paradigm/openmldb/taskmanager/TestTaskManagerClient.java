/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.taskmanager;

import com._4paradigm.openmldb.proto.TaskManager;
import com._4paradigm.openmldb.taskmanager.client.TaskManagerClient;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import java.util.List;

public class TestTaskManagerClient {
    TaskManagerServer server;
    TaskManagerClient client;

    @BeforeClass
    public void setUp() {
        server = new TaskManagerServer();
        Thread serverThread = new Thread() {
            public void run(){
                server.start();
            }
        };
        serverThread.start();
        client = new TaskManagerClient("127.0.0.1:9902");
    }

    @AfterClass
    public void tearDownAfterClass() {
        client.close();
        client = null;
        server.shutdown();
        server = null;
    }

    @Test
    public void testShowJobs() {
        try {
            List<TaskManager.JobInfo> jobInfos = client.showJobs();
            assert(jobInfos != null);
        } catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
    }

    @Test
    public void testPrintJobs() {
        try {
            client.printJobs();
        } catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
    }

    @Test
    public void testShowBatchVersion() {
        try {
            // TODO: Set path of batchjob jar and check if job changes to FINISH
            int id = client.showBatchVersion();
            assert(id > 0);
        } catch (Exception e) {
            e.printStackTrace();
            assert(false);
        }
    }

}

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
import com._4paradigm.openmldb.taskmanager.config.ConfigException;
import com._4paradigm.openmldb.taskmanager.server.TaskManagerServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.util.List;

@Ignore
public class TestTaskManagerClient {
    TaskManagerServer server;
    TaskManagerClient client;

    @BeforeClass
    public void setUp() throws ConfigException, IOException {
        server = new TaskManagerServer();
        server.startRpcServer(false);
        client = new TaskManagerClient("127.0.0.1:9999");
    }

    @AfterClass
    public void tearDownAfterClass() {
        client.close();
        server.close();
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

    @Ignore
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

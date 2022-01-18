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

import com._4paradigm.openmldb.taskmanager.client.TaskManagerClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

public class TestTaskManagerClient {

    TaskManagerClient client;

    @BeforeClass
    public void setUp() {
        if (client == null) {
            client = new TaskManagerClient("127.0.0.1:9902");
        }
    }

    @AfterClass
    public void tearDownAfterClass() {
        if (client != null) {
            client.close();
        }
        client = null;
    }

    @Test
    public void testShowJobs() {
        try {
            client.showJobs();
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

        /*
        client.dropOfflineTable("db_name","table_name");
        client.importOnlineData("load data infile 'file:///tmp/test.csv' " +
                "into table test_taskmanager.t1 options(format='csv', foo='bar', header=false, mode='append');");
        client.showJobs();
        client.printJobs();
        client.importOfflineData("load data infile 'file:///tmp/test.csv' into table test_taskmanager.t1 options(format='csv', foo='bar', header=false, mode='overwrite');");
        client.exportOfflineData("select * from test_taskmanager.t1 into outfile 'file:///tmp/test_csv2' options(format='csv', foo='bar', mode='overwrite');");
        client.showBatchVersion();
        */
    }
}

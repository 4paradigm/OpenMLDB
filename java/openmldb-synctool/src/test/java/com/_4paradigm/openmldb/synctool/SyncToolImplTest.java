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

package com._4paradigm.openmldb.synctool;

import com._4paradigm.openmldb.proto.DataSync;
import com._4paradigm.openmldb.proto.DataSync.TaskStatusResponse;
import com._4paradigm.openmldb.sdk.SqlException;

import junit.framework.TestCase;
import org.junit.Assert;

public class SyncToolImplTest extends TestCase {
    public void testHelp() {
        SyncTool.main(new String[] { "--help" });
    }

    public void testSyncToolImpl() {
        SyncTool tool = new SyncTool();
        try {
            tool.start(false);
        } catch (SqlException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            // default mode is FULL
            // DataSync.CreateSyncTaskRequest request = DataSync.CreateSyncTaskRequest.newBuilder().setDb("disk_test")
            //         .setName("t1").build();
            // tool.getSyncToolService().CreateSyncTask(request);
            // Thread.sleep(2000);
            // request = DataSync.CreateSyncTaskRequest.newBuilder().setDb("disk_test").setName("t2")
            //         .setMode(DataSync.SyncMode.FULL_AND_CONTINUOUS).build();
            // tool.getSyncToolService().CreateSyncTask(request);
            // TaskStatusResponse tasks = tool.getSyncToolService()
            //         .TaskStatus(DataSync.TaskStatusRequest.newBuilder().build());
            // // Assert.assertEquals(tasks.getTaskCount(), 1);

            // TODO(hw): tmp
            Thread.sleep(90000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tool.stop();
    }
}

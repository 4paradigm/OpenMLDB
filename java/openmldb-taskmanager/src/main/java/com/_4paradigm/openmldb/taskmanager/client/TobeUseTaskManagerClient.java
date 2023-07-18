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

package com._4paradigm.openmldb.taskmanager.client;

import java.util.ArrayList;

public class TobeUseTaskManagerClient {

    public static void main(String[] argv) throws Exception {

        TaskManagerClient client = new TaskManagerClient("127.0.0.1:9902");

        /*

        String sql = "select 1, 2";
        String output = client.runBatchSql(sql);
        System.out.println(output);
*/

        /*
        ArrayList<String> versions = client.getVersion();
        System.out.println("TaskManager version: " + versions.get(0));
        System.out.println("Batch version: " + versions.get(1));
*/

        String sql = "CREATE TABLE db1.t20 LIKE HIVE 'hive://test_database.test_inner_table3'";
        String output = client.runBatchSql(sql);
        System.out.println(output);


        client.stop();

    }


}

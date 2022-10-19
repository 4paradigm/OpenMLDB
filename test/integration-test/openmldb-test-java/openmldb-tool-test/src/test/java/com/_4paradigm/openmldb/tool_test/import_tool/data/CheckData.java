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
package com._4paradigm.openmldb.tool_test.import_tool.data;



import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBClient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class CheckData {
    public void testImportDataRight() throws Exception {
        String filePath = "data/smoke.csv";
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line = null;
        int count = 0;
        while((line = in.readLine())!=null){
            count++;
            if(count==1) continue;
            String[] data = line.split(",");

        }
        OpenMLDBClient fedbClient = new OpenMLDBClient("172.24.4.55:10015","/openmldb");

    }
}

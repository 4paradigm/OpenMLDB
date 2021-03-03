/*
 * TestDBMS.java
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

package com._4paradigm.fesql.jdbc;
import com._4paradigm.fesql.sdk.DBMSSdk;
import com._4paradigm.fesql.sdk.Status;
import com._4paradigm.fesql_interface;

public class TestDBMS {
    public static void main(String[] args) {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("mac")) {
            String path = TestDBMS.class.getResource("/libfesql_jsdk.jnilib").getPath();
            System.load(path);
        }else {
            String path = TestDBMS.class.getResource("/libfesql_jsdk.so").getPath();
            System.load(path);
        }
        String endpoint="172.27.128.37:9211";
        DBMSSdk sdk = fesql_interface.CreateDBMSSdk(endpoint);
        Status status = new Status();
        sdk.CreateDatabase("testxx", status);
    }
}

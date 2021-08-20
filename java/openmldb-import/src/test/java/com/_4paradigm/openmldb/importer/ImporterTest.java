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

package com._4paradigm.openmldb.importer;

import junit.framework.TestCase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ImporterTest extends TestCase {

    public void testHash() {
        List<String> keys = Arrays.asList("2|1", "1|1", "1|4", "2/6", "4", "6", "1");

        List<Integer> results = new ArrayList<>();
        for (String key : keys) {
            results.add(MurmurHash.hash32(key.getBytes(), key.length(), 0xe17a1465) % 8);
        }
        // c++ hash result
        List<Integer> expected = Arrays.asList(0, 2, 3, 5, 1, 4, 6);
        Assert.assertArrayEquals(expected.toArray(), results.toArray());
    }

    public void testHelp() {
        Importer.main(new String[]{"--help"});
    }

    // TODO(hw): run this test instead of running main class, just for simplifying.
    public void testMain() {
        Importer.main(new String[]{"--zk_cluster", "127.0.0.1:4181", "--db", "testdb", "--table", "t1", "--create_ddl", "create table t1 (\n" +
                "id string,\n" +
                "vendor_id int,\n" +
                "pickup_datetime timestamp,\n" +
                "dropoff_datetime timestamp,\n" +
                "passenger_count int,\n" +
                "pickup_longitude double,\n" +
                "pickup_latitude double,\n" +
                "dropoff_longitude double,\n" +
                "dropoff_latitude double,\n" +
                "store_and_fwd_flag string,\n" +
                "trip_duration int,\n" +
                "index(key=id, ts=pickup_datetime),\n" +
                "index(key=(vendor_id, passenger_count), ts=pickup_datetime),\n" +
                "index(key=passenger_count, ts=dropoff_datetime)\n" +
                ") OPTIONS (partitionnum=1);",
                "--files", "src/test/resources/train.csv.small",
//                "--rpc_size_limit", "20971520", // 20MB
                "-f",
                "--importer_mode", "bulkload"
        });
    }
}

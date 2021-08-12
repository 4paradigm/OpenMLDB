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

import java.util.Arrays;
import java.util.List;

public class ImporterTest extends TestCase {

    public void testHash() {
        List<String> keys = Arrays.asList("2|1", "1|1", "1|4", "2/6", "4", "6", "1");
        for (String key : keys) {
            System.out.println(MurmurHash.hash32(key.getBytes(), key.length(), 0xe17a1465) % 8);
        }
//        c++ hash result
//        I0715 21:37:10.869352 13732 tablet_impl_test.cc:5643] hash(2|1) = 0
//        I0715 21:37:10.869369 13732 tablet_impl_test.cc:5643] hash(1|1) = 2
//        I0715 21:37:10.869380 13732 tablet_impl_test.cc:5643] hash(1|4) = 3
//        I0715 21:37:10.869390 13732 tablet_impl_test.cc:5643] hash(2/6) = 5
//        I0715 21:37:10.869401 13732 tablet_impl_test.cc:5643] hash(4) = 1
//        I0715 21:37:10.869412 13732 tablet_impl_test.cc:5643] hash(6) = 4
//        I0715 21:37:10.869423 13732 tablet_impl_test.cc:5643] hash(1) = 6
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
                "index(key=id, ts=pickup_datetime)) OPTIONS (partitionnum=8" +
//                    "index(key=(vendor_id, passenger_count), ts=pickup_datetime),\n" +
//                    "index(key=passenger_count, ts=dropoff_datetime))\n" +
                ");",
                "--files", "src/test/resources/train.csv.small, /home/huangwei/NYCTaxiDataset/train.csv",
                "--rpc_size_limit", "20971520", // 20MB
                "-f",
                "--importer_mode", "bulkload"
        });
    }
}

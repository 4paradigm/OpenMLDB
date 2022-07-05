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
package com._4paradigm.openmldb.java_sdk_test.temp;


import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFactory;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;

public class TestCommand {
    @Test
    public void test1(){
        FEDBInfo fedbInfo = FEDBInfo.builder()
                .basePath("/home/zhaowei01/fedb-auto-test/0.1.5")
                .fedbPath("/home/zhaowei01/fedb-auto-test/0.1.5/openmldb-ns-1/bin/openmldb")
                .zk_cluster("172.24.4.55:10000")
                .zk_root_path("/openmldb")
                .nsNum(2).tabletNum(3)
                .nsEndpoints(Lists.newArrayList("172.24.4.55:10001", "172.24.4.55:10002"))
                .tabletEndpoints(Lists.newArrayList("172.24.4.55:10003", "172.24.4.55:10004", "172.24.4.55:10005"))
                .apiServerEndpoints(Lists.newArrayList("172.24.4.55:10006"))
                .build();
        // String command = OpenmlDBCommandFactory.getNoInteractiveCommand(fedbInfo, "test_zw", "desc t3");
        // System.out.println("command = " + command);
        List<String> test_zw = OpenMLDBCommandFactory.runNoInteractive(fedbInfo, "test_zw", "desc t4;");
        System.out.println("=======");
        test_zw.forEach(System.out::println);
    }
}

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

package com._4paradigm.fesql_auto_test.temp;

import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.util.FEDBDeploy;
import org.testng.annotations.Test;

public class TestFEDBDeploy {
    @Test
    public void test1(){
        FEDBDeploy deploy = new FEDBDeploy("2021-02-06");
        deploy.deployFEDB(2,3);
    }
    @Test
    public void test2(){
        FEDBDeploy deploy = new FEDBDeploy("fedb");
        FEDBInfo fedbInfo = deploy.deployFEDB(2, 3);
        System.out.println(fedbInfo);
    }
}

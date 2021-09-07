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

package com._4paradigm.openmldb.java_sdk_test.performance;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class ManualPerformanceTest {
    @Test
    public void requestAggExample8ThreadTest() {

        boolean enablePerformanceTest = Boolean.parseBoolean(System.getenv("ENABLE_MANUAL_TEST"));

        if (enablePerformanceTest) {
            RequestAggExample.run(RequestAggExample.ExecuteType.kRequestAgg, 8);
        }
    }

    @Test
    public void requestInsertExample8ThreadTest() {
        boolean enablePerformanceTest = Boolean.parseBoolean(System.getenv("ENABLE_MANUAL_TEST"));
        if (enablePerformanceTest) {
            RequestAggExample.run(RequestAggExample.ExecuteType.kRequestInsert, 8);
        }
    }

    @Test
    public void MixerWorkLoadExample() {
        boolean enablePerformanceTest = Boolean.parseBoolean(System.getenv("ENABLE_MANUAL_TEST"));
        if (enablePerformanceTest) {
            FESQLMixerWorkloadExample.run();
        }
    }
}

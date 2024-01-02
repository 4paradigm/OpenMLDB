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
package com._4paradigm.openmldb.common.zk;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ZKConfig {
    private String cluster;
    private String namespace;
    @Builder.Default
    private int sessionTimeout = 5000;
    @Builder.Default
    private int connectionTimeout = 5000;
    @Builder.Default
    private int maxRetries = 10;
    @Builder.Default
    private int baseSleepTime = 1000;
    @Builder.Default
    private int maxConnectWaitTime = 30000;
    @Builder.Default
    private String cert = "";

}

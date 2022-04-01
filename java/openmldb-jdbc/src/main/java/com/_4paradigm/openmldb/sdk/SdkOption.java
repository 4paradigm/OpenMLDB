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

package com._4paradigm.openmldb.sdk;

import lombok.Data;


@Data
public class SdkOption {
//    options for cluster mode
    private String zkCluster;
    private String zkPath;
//    options for standalone mode
    private String host;
    private long port;

    private long sessionTimeout = 10000;
    private Boolean enableDebug = false;
    private long requestTimeout = 60000;
    private boolean isClusterMode = true;
}

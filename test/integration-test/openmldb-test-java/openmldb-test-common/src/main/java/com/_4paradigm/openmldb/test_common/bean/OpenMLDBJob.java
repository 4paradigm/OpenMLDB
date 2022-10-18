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
package com._4paradigm.openmldb.test_common.bean;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class OpenMLDBJob {
    private int id;
    private String jobType;
    private String state;
    private Timestamp startTime;
    private Timestamp endTime;
    private String parameter;
    private String cluster;
    private String applicationId;
    private String error;
}

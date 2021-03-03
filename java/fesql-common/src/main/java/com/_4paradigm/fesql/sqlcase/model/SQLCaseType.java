/*
 * java/fesql-common/src/main/java/com/_4paradigm/fesql/sqlcase/model/SQLCaseType.java
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

package com._4paradigm.fesql.sqlcase.model;

import lombok.Getter;

/**
 * @author zhaowei
 * @date 2021/2/20 9:12 AM
 */
public enum SQLCaseType {
    kDDL("DDL"),                       //执行DDL
    kBatch("BATCH"),                     //在线批量查询
    kRequest("REQUEST"),                   //请求模式
    kBatchRequest("BATCH_REQUEST"),              //批量请求模式
    kRequestWithSp("REQUEST_WITH_SP"),             //
    kRequestWithSpAsync("REQUEST_WITH_SP_ASYNC"),
    kBatchRequestWithSp("BATCH_REQUEST_WITH_SP"),
    kBatchRequestWithSpAsync("BATCH_REQUEST_WITH_SP_ASYNC"),
    kDiffBatch("DIFF_BATCH"),
    kDiffRequest("DIFF_REQUEST"),
    kDiffRequestWithSp("DIFF_REQUEST_WITH_SP"),
    kDiffRequestWithSpAsync("DIFF_REQUEST_WITH_SP_ASYNC"),
    ;
    @Getter
    private String typeName;
    SQLCaseType(String typeName){
        this.typeName = typeName;
    }
}

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

package com._4paradigm.openmldb.test_common.model;

import com._4paradigm.openmldb.test_common.bean.OfflineInfo;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ExpectDesc extends Table {
    private String order;
    private int count = -1;
    private Boolean success = true;
    private Map<String,Object> options;
    private List<TableIndex> idxs;
    private int indexCount = -1;
    private OpenmldbDeployment deployment;
    private OpenmldbDeployment deploymentContains;
    private int deploymentCount = -1;
    private List<String> diffTables;
    private CatFile cat;
    private String msg;
    private PreAggTable preAgg;
    private List<PreAggTable> preAggList;
    private OfflineInfo offlineInfo;
    private List<String> offlineColumns;
    private List<List<Object>> offlineRows;
}

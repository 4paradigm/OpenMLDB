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

import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import com.google.common.base.Joiner;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/15 11:36 AM
 */
@Data
public class OpenMLDBResult {
    private String dbName;
    private List<String> tableNames;
    private String spName;
    private String sql;
    private boolean haveResult;
    private boolean ok;
    private int count;
    private String msg = "";
    private List<List<Object>> result;
    private List<String> columnNames;
    private List<String> columnTypes;
    private OpenMLDBTable schema;
    private OpenmldbDeployment deployment;
    private List<OpenmldbDeployment> deployments;
    private Integer deploymentCount;
    private OpenMLDBJob openMLDBJob;
    private List<String> offlineColumns;
    private List<List<Object>> offlineResult;
    private List<Map<String, Object>> formattedExpectResults;
    private List<Map<String, Object>> formattedActualResults;


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("OpenMLDBResult{");
        builder.append("sql=").append(sql);
        builder.append(", ok=").append(ok);
        if (!ok) {
            builder.append(", msg=").append(msg);
        }
        builder.append(", count=").append(count);
        builder.append(", openMLDBJob=").append(openMLDBJob);
        builder.append("}");
        if (result != null) {
            builder.append("result=" + result.size() + ":\n");
            if(CollectionUtils.isNotEmpty(columnTypes)){
                for(int i=0;i<columnNames.size();i++){
                    builder.append(columnNames.get(i))
                            .append(" ")
                            .append(columnTypes.get(i))
                            .append(",");
                }
            }else{
                if(CollectionUtils.isNotEmpty(columnNames)) {
                    for (int i = 0; i < columnNames.size(); i++) {
                        builder.append(columnNames.get(i))
                                .append(",");
                    }
                }
            }
            builder.append("\n");
            for (int i = 0; i < result.size(); i++) {
                List list = result.get(i);
                builder.append(Joiner.on(",").useForNull("null(obj)").join(list)).append("\n");
            }
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenMLDBResult that = (OpenMLDBResult) o;
        boolean flag = toString().equals(that.toString());
        return flag;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}

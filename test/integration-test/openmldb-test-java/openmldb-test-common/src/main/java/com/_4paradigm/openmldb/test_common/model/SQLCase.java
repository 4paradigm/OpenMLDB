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

import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class SQLCase implements Serializable{
    private String caseFileName;
    private int level = 0;
    private String id;
    private String desc;
    private String mode;
    private String json;
    private String db;
    private String version;
    private String longWindow;
    private String sql;
    private List<List<String>> dataProvider;
    private List<String> sqls;
    private boolean standard_sql;
    private boolean standard_sql_compatible;
    private List<String> tags;
    private List<String> common_column_indices;
    private String batch_plan;
    private String request_plan;
    private String cluster_request_plan;
    private List<InputDesc> inputs;
    private InputDesc batch_request;
    private ExpectDesc expect;
    private String spName = genAutoName();
    private UnequalExpect unequalExpect;
    // ANSISQL  HybridSQL  SQLITE3 MYSQL
    private List<String> sqlDialect;
    private InputDesc parameters;

    private Map<Integer, ExpectDesc> expectProvider;
    private List<String> tearDown;
    private List<String> excludes;
    private String only;
    private List<SQLCase> steps;

    public static String formatSql(String sql, int idx, String name) {
        return sql.replaceAll("\\{" + idx + "\\}", name);
    }

    public int getLevel() {
        return this.level;
    }

    public static String formatSql(String sql, String name) {
        return sql.replaceAll("\\{auto\\}", name);
    }

    public String getSql() {
        if(StringUtils.isNotEmpty(sql)){
            sql = formatSql(sql, Table.genAutoName());
            if (CollectionUtils.isEmpty(inputs)) {
                return sql;
            }
            for (int idx = 0; idx < inputs.size(); idx++) {
                sql = formatSql(sql, idx, inputs.get(idx).getName());
            }
        }
        return sql;
    }

    public static String genAutoName() {
        return "auto_" + RandomStringUtils.randomAlphabetic(8);
    }

    public boolean isSupportDiskTable(){
        if(CollectionUtils.isEmpty(inputs)){
            return false;
        }
        for(InputDesc input:inputs){
            if (CollectionUtils.isNotEmpty(input.getColumns())&& StringUtils.isEmpty(input.getCreate())&&StringUtils.isEmpty(input.getStorage())) {
                return true;
            }
        }
        return false;
    }
    public void setStorage(String storageMode){
        if(CollectionUtils.isNotEmpty(inputs)) {
            inputs.forEach(t -> {
                if(StringUtils.isEmpty(t.getStorage())){
                    t.setStorage(storageMode);
                }
            });
        }
    }

    public String getProcedure(String sql) {
        return buildCreateSpSQLFromColumnsIndexs(spName, sql, inputs.get(0).getColumns());
    }

    public static String buildCreateSpSQLFromColumnsIndexs(String name, String sql, List<String> columns) {
        if (sql == null || sql.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder("create procedure " + name + "(\n");
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String[] ss = column.split("\\s+");
            builder.append(ss[0]+" "+ss[1]);
            if (i != columns.size() - 1) {
                builder.append(",");
            }
        }
        builder.append(")\n");
        builder.append("BEGIN\n");
        builder.append(sql);
        builder.append("\n");
        builder.append("END;");
        sql = builder.toString();
        return sql;
    }

    public ExpectDesc getOnlineExpectByType(SQLCaseType sqlCaseType){
        ExpectDesc expect = this.getExpect();
        UnequalExpect unequalExpect = this.getUnequalExpect();
        if(unequalExpect==null) return expect;
        switch (sqlCaseType){
            case kDDL:
            case kBatch:
            case kSQLITE3:
            case kMYSQL:
            case kDIFFSQLITE3:
            case kDiffSQLResult:
                ExpectDesc batch_expect = unequalExpect.getBatch_expect();
                if(batch_expect!=null) return batch_expect;
                break;
            case kRequestWithSp:
            case kRequestWithSpAsync:
                ExpectDesc sp_expect = unequalExpect.getSp_expect();
                if(sp_expect!=null) return sp_expect;
            case kRequest:
                ExpectDesc request_expect = unequalExpect.getRequest_expect();
                if(request_expect!=null) return request_expect;
                break;
            case kBatchRequest:
            case kBatchRequestWithSp:
            case kBatchRequestWithSpAsync:
                ExpectDesc request_batch_expect = unequalExpect.getRequest_batch_expect();
                if(request_batch_expect!=null) return request_batch_expect;
                break;
        }
        ExpectDesc onlineExpect = unequalExpect.getOnline_expect();
        if(onlineExpect!=null) return onlineExpect;
        ExpectDesc euqalsExpect = unequalExpect.getExpect();
        if(euqalsExpect!=null) return euqalsExpect;
        return expect;
    }
    public ExpectDesc getOfflineExpectByType(){
        ExpectDesc expect = this.getExpect();
        UnequalExpect unequalExpect = this.getUnequalExpect();
        if(unequalExpect==null) return expect;
        ExpectDesc offlineExpect = unequalExpect.getOffline_expect();
        if(offlineExpect!=null) return offlineExpect;
        ExpectDesc euqalsExpect = unequalExpect.getExpect();
        if(euqalsExpect!=null) return euqalsExpect;
        return expect;
    }
}

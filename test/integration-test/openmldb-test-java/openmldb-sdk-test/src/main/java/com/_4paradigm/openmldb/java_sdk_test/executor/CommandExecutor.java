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
package com._4paradigm.openmldb.java_sdk_test.executor;


import com._4paradigm.openmldb.java_sdk_test.checker.Checker;
import com._4paradigm.openmldb.java_sdk_test.checker.CheckerStrategy;
import com._4paradigm.openmldb.java_sdk_test.checker.DiffVersionChecker;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFacade;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandUtil;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFactory;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class CommandExecutor extends BaseExecutor{

    protected Map<String, OpenMLDBInfo> openMLDBInfoMap;
    private Map<String, OpenMLDBResult> resultMap;

    public CommandExecutor(SQLCase sqlCase, SQLCaseType executorType) {
        this.sqlCase = sqlCase;
        this.executorType = executorType;
        dbName = sqlCase.getDb();
        if (!CollectionUtils.isEmpty(sqlCase.getInputs())) {
            for (InputDesc inputDesc : sqlCase.getInputs()) {
                tableNames.add(inputDesc.getName());
            }
        }
    }

    public CommandExecutor(SQLCase fesqlCase, Map<String, OpenMLDBInfo> openMLDBInfoMap, SQLCaseType executorType) {
        this(fesqlCase,executorType);
        this.openMLDBInfoMap = openMLDBInfoMap;
    }

    @Override
    public boolean verify() {
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("hybridse-only")) {
            log.info("skip case in cli mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("batch-unsupport")) {
            log.info("skip case in batch mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("rtidb-batch-unsupport")) {
            log.info("skip case in rtidb batch mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("rtidb-unsupport")) {
            log.info("skip case in rtidb mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("performance-sensitive-unsupport")) {
            log.info("skip case in rtidb mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("cli-unsupport")) {
            log.info("skip case in cli mode: {}", sqlCase.getDesc());
            return false;
        }
        return true;
    }

    @Override
    public void prepare(){
        prepare("mainVersion", OpenMLDBGlobalVar.mainInfo);
        if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
            openMLDBInfoMap.entrySet().stream().forEach(e -> prepare(e.getKey(), e.getValue()));
        }
    }

    protected void prepare(String version, OpenMLDBInfo openMLDBInfo){
        log.info("version:{} prepare begin",version);
        OpenMLDBResult openMLDBResult = OpenMLDBCommandUtil.createDB(openMLDBInfo,dbName);
        log.info("version:{},create db:{},{}", version, dbName, openMLDBResult.isOk());
        OpenMLDBResult res = OpenMLDBCommandUtil.createAndInsert(openMLDBInfo, dbName, sqlCase.getInputs());
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail . version:"+version);
        }
        log.info("version:{} prepare end",version);
    }

    @Override
    public void execute() {
        mainResult = execute("mainVersion", OpenMLDBGlobalVar.mainInfo);
        mainResult.setDbName(dbName);
        if(CollectionUtils.isNotEmpty(tableNames)) {
            mainResult.setTableNames(tableNames);
        }
        if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
            resultMap = openMLDBInfoMap.entrySet().stream().
                    collect(Collectors.toMap(e -> e.getKey(), e -> execute(e.getKey(), e.getValue())));
        }
    }

    protected OpenMLDBResult execute(String version, OpenMLDBInfo openMLDBInfo){
        log.info("version:{} execute begin",version);
        OpenMLDBResult openMLDBResult = null;
        List<String> sqls = sqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                // log.info("sql:{}", sql);
                if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                    sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
                }else {
                    sql = SQLUtil.formatSql(sql, tableNames);
                }
                openMLDBResult = OpenMLDBCommandFacade.sql(openMLDBInfo, dbName, sql);
            }
        }
        String sql = sqlCase.getSql();
        if (StringUtils.isNotEmpty(sql)) {
            // log.info("sql:{}", sql);
            if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
            }else {
                sql = SQLUtil.formatSql(sql, tableNames);
            }
            openMLDBResult = OpenMLDBCommandFacade.sql(openMLDBInfo, dbName, sql);
        }
        log.info("version:{} execute end",version);
        return openMLDBResult;
    }

    @Override
    public void check() throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(null,sqlCase, mainResult, executorType);
        if(MapUtils.isNotEmpty(resultMap)) {
            strategyList.add(new DiffVersionChecker(mainResult, resultMap));
        }
        for (Checker checker : strategyList) {
            checker.check();
        }
    }
    @Override
    public void tearDown() {
        tearDown("mainVersion", OpenMLDBGlobalVar.mainInfo);
        if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
            openMLDBInfoMap.entrySet().stream().forEach(e -> tearDown(e.getKey(), e.getValue()));
        }
    }


    public void tearDown(String version,OpenMLDBInfo openMLDBInfo) {
        log.info("version:{},begin tear down",version);
        List<String> tearDown = sqlCase.getTearDown();
        if(CollectionUtils.isNotEmpty(tearDown)){
            tearDown.forEach(sql->{
                if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                    sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
                }else {
                    sql = SQLUtil.formatSql(sql, tableNames);
                }
                OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo,dbName, sql);
            });
        }
        log.info("version:{},begin drop table",version);
        List<InputDesc> tables = sqlCase.getInputs();
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }
        for (InputDesc table : tables) {
            if(table.isDrop()) {
                String drop = "drop table " + table.getName() + ";";
                String db = table.getDb().isEmpty() ? dbName : table.getDb();
                OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo,db,drop);
            }
        }
    }
}

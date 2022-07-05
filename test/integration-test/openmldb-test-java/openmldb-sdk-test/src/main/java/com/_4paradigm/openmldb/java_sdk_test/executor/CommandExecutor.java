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
import com._4paradigm.openmldb.test_common.command.OpenMLDBComamndFacade;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandUtil;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFactory;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.util.OpenMLDBUtil;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class CommandExecutor extends BaseExecutor{

    private static final Logger logger = new LogProxy(log);
    protected Map<String, OpenMLDBInfo> openMLDBInfoMap;
    private Map<String, OpenMLDBResult> resultMap;

    public CommandExecutor(SQLCase fesqlCase, SQLCaseType executorType) {
        this.fesqlCase = fesqlCase;
        this.executorType = executorType;
        dbName = fesqlCase.getDb();
        if (!CollectionUtils.isEmpty(fesqlCase.getInputs())) {
            for (InputDesc inputDesc : fesqlCase.getInputs()) {
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
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("hybridse-only")) {
            logger.info("skip case in cli mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("batch-unsupport")) {
            logger.info("skip case in batch mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-batch-unsupport")) {
            logger.info("skip case in rtidb batch mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-unsupport")) {
            logger.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("performance-sensitive-unsupport")) {
            logger.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("cli-unsupport")) {
            logger.info("skip case in cli mode: {}", fesqlCase.getDesc());
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
        logger.info("version:{} prepare begin",version);
        OpenMLDBResult fesqlResult = OpenMLDBCommandUtil.createDB(openMLDBInfo,dbName);
        logger.info("version:{},create db:{},{}", version, dbName, fesqlResult.isOk());
        OpenMLDBResult res = OpenMLDBCommandUtil.createAndInsert(openMLDBInfo, dbName, fesqlCase.getInputs());
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail . version:"+version);
        }
        logger.info("version:{} prepare end",version);
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
        logger.info("version:{} execute begin",version);
        OpenMLDBResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                // log.info("sql:{}", sql);
                if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                    sql = OpenMLDBUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
                }else {
                    sql = OpenMLDBUtil.formatSql(sql, tableNames);
                }
                fesqlResult = OpenMLDBComamndFacade.sql(openMLDBInfo, dbName, sql);
            }
        }
        String sql = fesqlCase.getSql();
        if (StringUtils.isNotEmpty(sql)) {
            // log.info("sql:{}", sql);
            if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                sql = OpenMLDBUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
            }else {
                sql = OpenMLDBUtil.formatSql(sql, tableNames);
            }
            fesqlResult = OpenMLDBComamndFacade.sql(openMLDBInfo, dbName, sql);
        }
        logger.info("version:{} execute end",version);
        return fesqlResult;
    }

    @Override
    public void check() throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(fesqlCase, mainResult, executorType);
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
        logger.info("version:{},begin tear down",version);
        List<String> tearDown = fesqlCase.getTearDown();
        if(CollectionUtils.isNotEmpty(tearDown)){
            tearDown.forEach(sql->{
                if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                    sql = OpenMLDBUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
                }else {
                    sql = OpenMLDBUtil.formatSql(sql, tableNames);
                }
                OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo,dbName, sql);
            });
        }
        logger.info("version:{},begin drop table",version);
        List<InputDesc> tables = fesqlCase.getInputs();
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

package com._4paradigm.fesql_auto_test.executor.diff;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.executor.SQLExecutor;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class DiffVersionRequestExecutor extends DiffVersionSQLExecutor {

    protected boolean isBatchRequest;
    protected boolean isAsyn;

    public DiffVersionRequestExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap,
                                      boolean isBatchRequest, boolean isAsyn) {
        super(fesqlCase, executor, executorMap, fedbInfoMap);
        dbName = fesqlCase.getDb();
        if (!CollectionUtils.isEmpty(fesqlCase.getInputs())) {
            for (InputDesc inputDesc : fesqlCase.getInputs()) {
                tableNames.add(inputDesc.getName());
            }
        }
        this.isBatchRequest = isBatchRequest;
        this.isAsyn = isAsyn;
    }

    @Override
    public void prepare(String version,SqlExecutor executor) {
        log.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        boolean useFirstInputAsRequests = !isBatchRequest && null == fesqlCase.getBatch_request();
        FesqlResult res = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), useFirstInputAsRequests);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run SQLExecutor: prepare fail");
        }
        log.info("version:{} prepare end",version);
    }

    @Override
    public boolean verify() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("request-unsupport")) {
            log.info("skip case in request mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-unsupport")) {
            log.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-request-unsupport")) {
            log.info("skip case in rtidb request mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (FesqlConfig.isCluster() &&
                null != fesqlCase.getMode() && fesqlCase.getMode().contains("cluster-unsupport")) {
            log.info("cluster-unsupport, skip case in cluster request mode: {}", fesqlCase.getDesc());
            return false;
        }
        return true;
    }

    @Override
    public FesqlResult execute(String version, SqlExecutor executor) {
        log.info("version:{} execute begin",version);
        FesqlResult fesqlResult = null;
        try {
            List<String> sqls = fesqlCase.getSqls();
            if (sqls != null && sqls.size() > 0) {
                for (String sql : sqls) {
                    // log.info("sql:{}", sql);
                    sql = FesqlUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
                    fesqlResult = FesqlUtil.sql(executor, dbName, sql);
                }
            }
            String sql = fesqlCase.getSql();
            if (sql != null && sql.length() > 0) {
                log.info("sql:{}", sql);
                sql = FesqlUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
                InputDesc request = null;
                if (isBatchRequest) {
                    InputDesc batchRequest = fesqlCase.getBatch_request();
                    if (batchRequest == null) {
                        log.error("No batch request provided in case");
                        return null;
                    }
                    List<Integer> commonColumnIndices = new ArrayList<>();
                    if (batchRequest.getCommon_column_indices() != null) {
                        for (String str : batchRequest.getCommon_column_indices()) {
                            if (str != null) {
                                commonColumnIndices.add(Integer.parseInt(str));
                            }
                        }
                    }

                    fesqlResult = FesqlUtil.sqlBatchRequestMode(
                            executor, dbName, sql, batchRequest, commonColumnIndices);
                } else {
                    if (null != fesqlCase.getBatch_request()) {
                        request = fesqlCase.getBatch_request();
                    } else if (!fesqlCase.getInputs().isEmpty()) {
                        request = fesqlCase.getInputs().get(0);
                    }
                    if (null == request || CollectionUtils.isEmpty(request.getColumns())) {
                        log.error("fail to execute in request query sql executor: sql case request columns is empty");
                        return null;
                    }
                    fesqlResult = FesqlUtil.sqlRequestMode(executor, dbName, null == fesqlCase.getBatch_request(), sql, request);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        log.info("version:{} execute end",version);
        return fesqlResult;
    }
}

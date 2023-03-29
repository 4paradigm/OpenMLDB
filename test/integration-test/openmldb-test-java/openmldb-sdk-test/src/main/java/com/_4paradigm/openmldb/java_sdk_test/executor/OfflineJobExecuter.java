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

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBConfig;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.test_tool.command_tool.common.ExecUtil;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;


@Slf4j
public class OfflineJobExecuter extends BaseSQLExecutor {
    private SDKClient sdkClient;
    private boolean useFirstInputAsRequests = false;
    private Map<String, OpenMLDBResult> resultMap;
    private String offlineDataPrefix = "/tmp/openmldb_offline_storage/";


    public OfflineJobExecuter(SqlExecutor executor, SQLCase sqlCase, SQLCaseType executorType) {
        super(executor, sqlCase, executorType);
        sdkClient = SDKClient.of(executor);
    }
    public OfflineJobExecuter(SQLCase sqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> fedbInfoMap, SQLCaseType executorType) {
        super(sqlCase, executor, executorMap, fedbInfoMap, executorType);
    }

    @Override
    public boolean verify() {
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("hybridse-only")) {
            log.info("skip case in batch mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("batch-unsupport")) {
            log.info("skip case in batch mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && !OpenMLDBGlobalVar.tableStorageMode.equals("memory") && sqlCase.getMode().contains("disk-unsupport")) {
            log.info("skip case in disk mode: {}", sqlCase.getDesc());
            return false;
        }
        if (OpenMLDBConfig.isCluster() && null != sqlCase.getMode() && sqlCase.getMode().contains("cluster-unsupport")) {
            log.info("skip case in cluster mode: {}", sqlCase.getDesc());
            return false;
        }
        return true;
    }

    @Override
    public void prepare(String version,SqlExecutor executor){
        log.info("version:{} prepare begin",version);
        sdkClient.createAndUseDB(dbName);
        sdkClient.execute("SET @@global.sync_job = 'true';");
        sdkClient.execute("SET @@global.job_timeout = '600000';");
        sdkClient.setOffline();
        ExecUtil.exeCommand("touch "+offlineDataPrefix);

        List<InputDesc> inputs = sqlCase.getInputs();

        if (inputs != null && inputs.size() > 0) {

            for (int i = 0; i < inputs.size(); i++) {
                InputDesc input = inputs.get(i);
                if (StringUtils.isNotEmpty(input.getDb())) {
                    sdkClient.createAndUseDB(input.getDb());
                }else{
                    sdkClient.useDB(dbName);
                }
                String tableName = input.getName();
                String createSql = input.extractCreate();
                if(StringUtils.isEmpty(createSql)){
                    continue;
                }
                createSql = SQLCase.formatSql(createSql, i, tableName);
                createSql = SQLUtil.formatSql(createSql, OpenMLDBGlobalVar.mainInfo);
                sdkClient.execute(createSql);
                if (useFirstInputAsRequests && i==0) {
                    continue;
                }

                List<List<Object>> inserts = input.getRows();
                if (inserts==null){
                    log.info("version:{} prepare end",version);
                    break;
                }

                // write inputs to csv file
                String filePath = offlineDataPrefix+tableName+ ".csv";
                ExecUtil.exeCommand("touch "+filePath);
                try {
                    BufferedWriter bufferedWriter = new BufferedWriter(
                            new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8"));
                    String tmp = input.getColumns().toString();
                    bufferedWriter.write(tmp.substring(1, tmp.length()-1));
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    for (Object insert : inserts) {
                        tmp = insert.toString().replace(" ","");
                        tmp.replace(",,", ", ,");
                        bufferedWriter.write(tmp.substring(1, tmp.length()-1));
                        bufferedWriter.newLine();
                        bufferedWriter.flush();
                    }
                    bufferedWriter.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } 
                // load data in offline engine               
                String insertSql = "load data infile 'file://"+filePath+"' into table "+tableName+" options(mode='overwrite')";
                        OpenMLDBResult openMLDBResult = sdkClient.execute(insertSql);
//                        OpenMLDBResult res = SDKUtil.insert(executor, dbName, insertSql);
                        if (!openMLDBResult.isOk()) {
                            throw new RuntimeException("prepare insert fail, version:"+version+",openMLDBResult:"+openMLDBResult);
                        }                  

        log.info("version:{} prepare end",version);
        }
    }

}

    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor){
        log.info("version:{} execute begin",version);
        sdkClient.useDB(dbName);
        OpenMLDBResult openMLDBResult = null;
        List<String> sqls = sqlCase.getSqls();
        long totalMilliSeconds = System.currentTimeMillis();
        String outDirPath = offlineDataPrefix+Long.toString(totalMilliSeconds);
//        ExecUtil.exeCommand("touch "+outDirPath);
        if (CollectionUtils.isNotEmpty(sqls)) {
            for (String sql : sqls) {
                sql = MapUtils.isNotEmpty(openMLDBInfoMap)?SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version)):SQLUtil.formatSql(sql, tableNames);
                sql = SQLUtil.formatSql(sql);
                sql = sql.split(";")[0] +" INTO OUTFILE '"+outDirPath+"' OPTIONS ( format = 'parquet' );";
                openMLDBResult = sdkClient.execute(sql);
            }
        }
        String sql = sqlCase.getSql();
        if (StringUtils.isNotEmpty(sql)) {
            sql = MapUtils.isNotEmpty(openMLDBInfoMap)?SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version)):SQLUtil.formatSql(sql, tableNames);
            sql = SQLUtil.formatSql(sql);
            sql = sql.split(";")[0] +" INTO OUTFILE '"+outDirPath+"' OPTIONS ( format = 'parquet' );";
            openMLDBResult = sdkClient.execute(sql);
        }

        //add parquet results to openMLDBResult
        if (openMLDBResult.isOk()){
            log.info("start parse parquet");
            String parquetFile = ExecutorUtil.run("ls "+ outDirPath +" | grep .snappy.parquet").get(0);
            Path file = new Path(outDirPath+"/"+parquetFile);
            List<String> columns = new ArrayList<String>();
            List<List<Object>> offlineResult = new ArrayList<>();
            try {        
                ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
                GenericRecord record;
                int len = 0;
                int pos = 0;
                         
                while((record = reader.read()) != null) {
                    // set schema
                    if (pos==0){
                        len = record.getSchema().getFields().size(); 
                        for (int i=0;i<len;i++) {
                            columns.add(record.getSchema().getFields().get(i).name());
                        }
                    }
                    pos++;
                    List<Object> offlineRow = new ArrayList<>();                   
                    for (int i=0;i<len;i++){
                        offlineRow.add(record.get(i));
                    }
                    offlineResult.add(offlineRow);
                }
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            openMLDBResult.setOfflineResult(offlineResult);
            openMLDBResult.setResult(offlineResult);
            openMLDBResult.setOfflineColumns(columns);
        }
        log.info("version:{} execute end",version);
        return openMLDBResult;
    }

    // @Override
    // public void check() throws Exception {
    // }
}

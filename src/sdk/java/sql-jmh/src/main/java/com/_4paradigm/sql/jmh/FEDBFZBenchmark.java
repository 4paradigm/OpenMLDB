package com._4paradigm.sql.jmh;
import com._4paradigm.featuredb.driver.dto.DeployConfig;
import com._4paradigm.featuredb.driver.impl.DBMSDriverClientImpl;
import com._4paradigm.featuredb.proto.Base;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.featuredb.rtengine.driver.dto.feql.ExecuteResult;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.tools.Util;
import com._4paradigm.sql.tools.Relation;
import com._4paradigm.sql.tools.TableInfo;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.featuredb.driver.DBMSClient;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)

public class FEDBFZBenchmark {
    private SqlExecutor executor;
    private String db;
    private int pkNum = 1;
    //@Param({"500", "1000", "2000"})
    private int windowNum = 500;
    int pkBase = 1000000;
    private String script;
    private String nsName = "test_fz";
    private String deployName = "test_1";
    private DBMSClient dbmsClient = null;
    private TableSyncClient tableSyncClient = null;
    private NameServerClientImpl nsc = null;
    private RTIDBClusterClient cluster = null;

    private Map<String, TableInfo> tableMap;
    private Set<String> rtidbTables;
    private String mainTable;
    List<Map<String, Object>> mainTableValue;
    private Object[][] inputData;
    List<String> commonColumnIndices;

    public FEDBFZBenchmark() {
        nsName += String.valueOf(new Random().nextInt());
        RTIDBClientConfig config = new RTIDBClientConfig();
        config.setZkEndpoints(BenchmarkConfig.ZK_CLUSTER);
        config.setZkRootPath(BenchmarkConfig.ZK_PATH);
        config.setReadTimeout(10000);
        config.setWriteTimeout(10000);
        cluster = new RTIDBClusterClient(config);
        try {
            cluster.init();
            tableSyncClient = new TableSyncClientImpl(cluster);
            nsc = new NameServerClientImpl(config);
            nsc.init();
            dbmsClient = new DBMSDriverClientImpl(BenchmarkConfig.ZK_CLUSTER, BenchmarkConfig.ZK_NS);
            dbmsClient.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
        tableMap = new HashMap<>();
        rtidbTables = new HashSet<>();
        mainTableValue = new ArrayList<>();
        commonColumnIndices = new ArrayList<>();
    }

    public void createTable() {
        String json = Util.getContent(BenchmarkConfig.jsonUrl).trim();
        script = Util.getContent(BenchmarkConfig.scriptUrl).trim();
        Relation relation = new Relation(Util.getContent(BenchmarkConfig.relationUrl));
        mainTable = relation.getMainTable();
        try {
            dbmsClient.createNs(nsName);
            JSONObject jsonObject = new JSONObject(json);
            JSONObject tableInfo = jsonObject.getJSONObject("tableInfo");
            Iterator iter = tableInfo.keys();
            while(iter.hasNext()) {
                String name = (String)iter.next();
                Base.TableDesc.Builder tableBuilder = Base.TableDesc.newBuilder().setNs(nsName).setName(name);
                JSONArray schema = tableInfo.getJSONArray(name);
                for (int i = 0; i < schema.length(); i++) {
                    JSONObject val = schema.getJSONObject(i);
                    String field = val.getString("name");
                    String type = val.getString("type");
                    Base.ColumnDesc.Builder columnDescBuilder = Base.ColumnDesc.newBuilder();
                    columnDescBuilder.setName(field);
                    columnDescBuilder.setType(Util.getFeatureDBType(type));
                    tableBuilder.addColumns(columnDescBuilder.build());
                }
                Base.TableDesc table = tableBuilder.build();
                dbmsClient.createTable(table);
                tableMap.put(name, new TableInfo(name, schema, relation));
                if (mainTable.equals(name) && !BenchmarkConfig.commonCol.isEmpty()) {
                    String[] colArr = BenchmarkConfig.commonCol.trim().split(",");
                    for (String col : colArr) {
                        commonColumnIndices.add(col);
                    }
                }
            }
            //List<Table> tList = dbmsClient.showTable(nsName);
            //CompileResult compileResult = dbmsClient.compileFEQL(nsName, script);
            DeployConfig config = DeployConfig.builder().engineList(dbmsClient.showEngine(Base.EngineType.kRealTimeEngine)).build();
            config.setReplicaNum(1);
            config.setDebug(true);
            config.setPartitionNum(Integer.valueOf(BenchmarkConfig.PARTITION_NUM));
            if (BenchmarkConfig.mode == BenchmarkConfig.Mode.BATCH_REQUEST && !commonColumnIndices.isEmpty()) {
                config.setConstantColumns(commonColumnIndices);
            }
            dbmsClient.deployFEQL(nsName, script, deployName, config);
            Thread.sleep(1000);
            Map<String, List<String>> rtidbTableMap = dbmsClient.showDeploy(nsName, deployName).getRtidbTables();
            for (String name : rtidbTableMap.get(deployName)) {
                rtidbTables.add(name);
            }
        } catch (Exception e) {
            e.printStackTrace();
       }
    }

    public void putData() {
        TableInfo mainTableInfo = tableMap.get(mainTable);
        putTableData(mainTableInfo);
        for (Map.Entry<String, TableInfo> entry : tableMap.entrySet()) {
            if (entry.getKey().equals(mainTable)) {
                continue;
            }
            putTableData(entry.getValue());
        }
    }

    private void putTableData(TableInfo table) {
        boolean isMainTable = false;
        if (table.getName().equals(mainTable)) {
            isMainTable = true;
        }
        if (!isMainTable && mainTableValue.size() != pkNum) {
            return;
        }
        String rtidbTableName = nsName + "_" + table.getName();
        List<String> schema = table.getSchema();
        Set<Integer> index = table.getIndex();
        Set<Integer> tsIndex = table.getTsIndex();
        Map<Integer, String> relation = table.getColRelation();

        Map<String, Object> valueMap;
        Map<String, Object> val = new HashMap<>();
        for (int i = 0; i < pkNum; i++) {
            long ts = System.currentTimeMillis();
            if (isMainTable) {
                valueMap = new HashMap<>();
            } else {
                valueMap = mainTableValue.get(i);
            }
            for (int tsCnt = 0; tsCnt < windowNum; tsCnt++) {
                val.clear();
                for (int pos = 0; pos < schema.size(); pos++) {
                    String fieldName = table.getSchemaPosName().get(pos);
                    String type = schema.get(pos);
                    if (!isMainTable && index.contains(pos)) {
                        val.put(fieldName, valueMap.get(relation.get(pos)));
                        continue;
                    }
                    if (type.equals("string")) {
                        val.put(fieldName, "col" + pos);
                        if (index.contains(pos)) {
                            if (!valueMap.containsKey(fieldName)) {
                                valueMap.put(fieldName, "col" + pos);
                            }
                        }
                    } else if (type.equals("float")) {
                        val.put(fieldName, 1.3);
                    } else if (type.equals("double")) {
                        val.put(fieldName, 1.4d);
                    } else if (type.equals("bigint") || type.equals("timestamp") || type.equals("int") || type.equals("long")) {
                        if (type.equals("timestamp")) {
                            val.put(fieldName, new DateTime(ts - tsCnt * 1000));
                        } else if (type.equals("long") || type.equals("bigint")){
                            val.put(fieldName, (long)pos);
                        } else {
                            val.put(fieldName, pos);
                        }
                        if (index.contains(pos)) {
                            if (!valueMap.containsKey(fieldName)) {
                                valueMap.put(fieldName, val.get(fieldName));
                            }
                        }
                    } else if (type.equals("bool")) {
                        val.put(fieldName, true);
                    } else if (type.equals("date")) {
                        val.put(fieldName, new Date(2020, 12, 10));
                    } else {
                        System.out.println("invalid type");
                    }
                }
                if (isMainTable && inputData == null) {
                    Map<Integer, String> posName = table.getSchemaPosName();
                    if (BenchmarkConfig.mode == BenchmarkConfig.Mode.BATCH_REQUEST) {
                        inputData = new Object[BenchmarkConfig.BATCH_SIZE][schema.size()];
                    } else {
                        inputData = new Object[1][schema.size()];
                    }
                    for (int j = 0; j < inputData.length; j++) {
                        for (int k = 0; k < schema.size(); k++) {
                            inputData[j][k] = val.get(posName.get(k));
                        }
                    }
                }
                if (!rtidbTables.contains(table.getName())) {
                    continue;
                }
                try {
                    if (tsIndex.isEmpty()) {
                        tableSyncClient.put(rtidbTableName, ts - tsCnt, val);
                    } else {
                        tableSyncClient.put(rtidbTableName, val);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (isMainTable) {
                mainTableValue.add(valueMap);
            }
        }
    }

    @Setup
    public void setup() {
        createTable();
        putData();
    }

    @TearDown
    public void teardown() {
        /*List<com._4paradigm.rtidb.ns.NS.TableInfo> tables = nsc.showTable("");
        for (com._4paradigm.rtidb.ns.NS.TableInfo ta : tables) {
            rtidbTables.add(ta.getName());
        }*/
        for (String name : rtidbTables) {
            String tableName = nsName + "_" + name;
            nsc.dropTable(tableName);
        }
        try {
            List<Base.EngineStatus> list = dbmsClient.showEngine(Base.EngineType.kRealTimeEngine);
            for (Base.EngineStatus engine : list) {
                dbmsClient.dropFEQL(nsName, deployName, engine);
            }
            dbmsClient.dropDeploy(nsName, deployName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void execSQL() {
        //for (int i = 0; i < 100000; i++) {
            try {
                ExecuteResult result = dbmsClient.batchExecuteFEQL(nsName, deployName, inputData);
                int a = 0;
            } catch (Exception e) {
                e.printStackTrace();
            }
        //}
    }

    public void close() {
        dbmsClient.close();
        nsc.close();
        cluster.close();
    }

    public static void main(String[] args) throws RunnerException {
        FEDBFZBenchmark ben = new FEDBFZBenchmark();
        try {
            ben.setup();
            ben.execSQL();
            ben.teardown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ben.close();
       /* Options opt = new OptionsBuilder()
                .include(FEDBFZBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();*/
    }
}

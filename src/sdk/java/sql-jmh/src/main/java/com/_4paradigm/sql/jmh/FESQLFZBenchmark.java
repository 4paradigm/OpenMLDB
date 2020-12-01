package com._4paradigm.sql.jmh;

import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 1)

public class FESQLFZBenchmark {
    private SqlExecutor executor;
    private String db;
    private String ddlUrl = "http://172.27.128.37:8999/fz_ddl/batch_request100680.txt.ddl.txt";
    private String scriptUrl = "http://172.27.128.37:8999/fz_ddl/batch_request100680.txt";
    private String relationUrl = "http://172.27.128.37:8999/fz_ddl/batch_request100680.relation.txt";
    private int pkNum = 1;
    @Param({"500", "1000", "2000"})
    private int windowNum = 10;
    private Map<String, TableInfo> tableMap;
    private String script;
    private String mainTable;
    List<Map<String, String>> mainTableValue;
    int pkBase = 1000000;

    public FESQLFZBenchmark() {
        executor = BenchmarkConfig.GetSqlExecutor();
        db = "db" + System.nanoTime();
        tableMap = new HashMap<>();
        mainTableValue = new ArrayList<>();
    }

    class TableInfo {
        private String name;
        private String ddl;
        private Map<String, Integer> schemaPos;
        private Map<Integer, String> schemaPosName;
        private List<String> schema;
        private Set<Integer> tsIndex;
        private Set<Integer> index;
        private Map<Integer, String> relation;
        public TableInfo(String ddl, Map<String, Map<String, String>> relationMap) {
            this.ddl = ddl + ";";
            String[] arr = ddl.split("index\\(")[0].split("\\(");
            name = arr[0].split(" ")[2].replaceAll("`", "");
            String indexStr = relationMap.get(name).get("index");
            String tsIndexStr = relationMap.get(name).get("ts");

            String[] filed = arr[1].split(",");
            schema = new ArrayList<>();
            schemaPos = new HashMap<>();
            schemaPosName = new HashMap<>();
            for (int i = 0; i < filed.length; i++) {
                String[] tmp = filed[i].split(" ");
                if (tmp.length < 2) {
                    continue;
                }
                schema.add(tmp[1].trim());
                String fieldName = tmp[0].replaceAll("`", "");
                schemaPos.put(fieldName, i);
                schemaPosName.put(i, fieldName);
            }
            index = new HashSet<>();
            for (String val : indexStr.trim().split(",")) {
                String[] tmp = val.split("\\|");
                for (String field : tmp) {
                    index.add(schemaPos.get(field));
                }
            }
            tsIndex = new HashSet<>();
            if (!tsIndexStr.equals("null")) {
                for (String val : tsIndexStr.trim().split(",")) {
                    tsIndex.add(schemaPos.get(val));
                }
            }
            String relationStr = relationMap.get(name).get("relation");
            relation = new HashMap<>();
            if (!relationStr.equals("null")) {
                String[] val = relationStr.trim().split("\\|");
                if (val.length == 2) {
                    relation.put(schemaPos.get(val[1]), val[0]);
                }
            }
            if (name.equals(mainTable)) {
                for (Map.Entry<String, Map<String, String>> entry : relationMap.entrySet()) {
                    if (entry.getKey().equals(mainTable)) {
                        continue;
                    }
                    String curRelationStr = entry.getValue().get("relation");
                    String[] tmp = curRelationStr.trim().split("\\|");
                    index.add(schemaPos.get(tmp[0]));
                }
            }
        }

        public String getDDL() { return ddl; }
        public Set<Integer> getTsIndex() { return tsIndex; }
        public String getName() { return name; }
        public Set<Integer> getIndex() { return index; }
        public List<String> getSchema() { return schema; }
        public Map<String, Integer> getSchemaPos() { return schemaPos; }
        public Map<Integer, String> getRelation() { return relation; }
        public Map<Integer, String> getSchemaPosName() { return schemaPosName; }
    }

    public String getContent(String httpUrl) {
        try {
            URL url = new URL(httpUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.connect();
            if (con.getResponseCode() == 200) {
                InputStream is = con.getInputStream();
                StringBuilder builder = new StringBuilder();
                int len = 0;
                byte[] buffer = new byte[1024];
                while ((len = is.read(buffer)) != -1) {
                    byte[] temp = new byte[len];
                    System.arraycopy(buffer, 0 , temp, 0, len);
                    builder.append(new String(temp, "utf-8"));
                }
                return builder.toString();
            } else {
                System.out.println("request failed");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public void init() {
        String rawScript = getContent(scriptUrl);
        script = rawScript.trim().replace("\n", " ");
        String relation = getContent(relationUrl);
        String[] arr = relation.trim().split("\n");
        Map<String, Map<String, String>> relationMap = new HashMap<>();
        for (String item : arr) {
            String[] tmp = item.trim().split(" ");
            if (tmp.length < 5) {
                System.out.println("parse relation error");
                continue;
            }
            if (tmp[1].equals("null")) {
                mainTable = tmp[0];
            }
            Map<String, String> tableMap = new HashMap<>();
            tableMap.put("main", tmp[1]);
            tableMap.put("relation", tmp[2]);
            tableMap.put("index", tmp[3]);
            tableMap.put("ts", tmp[4]);
            relationMap.put(tmp[0], tableMap);
        }
        String ddl = getContent(ddlUrl);
        arr = ddl.split(";");
        for (String item : arr) {
            item = item.trim().replace("\n", "");
            if (item.isEmpty()) {
                continue;
            }
            TableInfo table = new TableInfo(item, relationMap);
            tableMap.put(table.getName(), table);
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
        List<String> schema = table.getSchema();
        Set<Integer> index = table.getIndex();
        Set<Integer> tsIndex = table.getTsIndex();
        Map<Integer, String> relation = table.getRelation();

        Map<String, String> valueMap;
        for (int i = 0; i < pkNum; i++) {
            long ts = System.currentTimeMillis();
            if (isMainTable) {
                valueMap = new HashMap<>();
            } else {
                valueMap = mainTableValue.get(i);
            }
            for (int tsCnt = 0; tsCnt < windowNum; tsCnt++) {
                StringBuilder builder = new StringBuilder();
                builder.append("insert into ");
                builder.append(table.getName());
                builder.append(" values(");
                for (int pos = 0; pos < table.schema.size(); pos++) {
                    if (pos > 0) {
                        builder.append(", ");
                    }
                    String type = schema.get(pos);
                    if (!isMainTable && index.contains(pos)) {
                        if (type.equals("string")) {
                            builder.append("'");
                        }
                        builder.append(valueMap.get(relation.get(pos)));
                        if (type.equals("string")) {
                            builder.append("'");
                        }
                        continue;
                    }
                    if (type.equals("string")) {
                        builder.append("'");
                        builder.append("col");
                        builder.append(pos);
                        if (index.contains(pos)) {
                            String fieldName = table.getSchemaPosName().get(pos);
                            if (!valueMap.containsKey(fieldName)) {
                                valueMap.put(fieldName, "col" + pos);
                            }
                        }
                        builder.append("'");
                    } else if (type.equals("float")) {
                        builder.append(1.3);
                    } else if (type.equals("double")) {
                        builder.append("1.4");
                    } else if (type.equals("bigint") || type.equals("timestamp") || type.equals("int")) {
                        if (index.contains(pos)) {
                            builder.append(pkBase + i);
                            String fieldName = table.getSchemaPosName().get(pos);
                            if (!valueMap.containsKey(fieldName)) {
                                valueMap.put(fieldName, String.valueOf(pkBase + i));
                            }
                        } else if (tsIndex.contains(pos)) {
                            builder.append(ts - tsCnt);
                        } else {
                            if (type.equals("timestamp")) {
                                builder.append(ts);
                            } else {
                                builder.append(pos);
                            }
                        }
                    } else if (type.equals("bool")) {
                        builder.append(true);
                    } else if (type.equals("date")) {
                        builder.append("'2020-11-27'");
                    } else {
                        System.out.println("invalid type");
                    }
                }
                builder.append(");");
                String exeSql = builder.toString();
                executor.executeInsert(db, exeSql);
            }
            if (isMainTable) {
                mainTableValue.add(valueMap);
            }
        }
    }

    private PreparedStatement getPreparedStatement() throws SQLException {
        PreparedStatement requestPs = executor.getRequestPreparedStmt(db, script);
        ResultSetMetaData metaData = requestPs.getMetaData();
        TableInfo table = tableMap.get(mainTable);
        if (table.getSchema().size() != metaData.getColumnCount()) {
            return null;
        }
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.VARCHAR) {
                requestPs.setString(i + 1, "col" + String.valueOf(i));
            } else if (columnType == Types.DOUBLE) {
                requestPs.setDouble(i + 1, 1.4d);
            } else if (columnType == Types.FLOAT) {
                requestPs.setFloat(i + 1, 1.3f);
            } else if (columnType == Types.INTEGER) {
                if (table.getIndex().contains(i)) {
                    requestPs.setInt(i + 1, pkBase + i);
                } else {
                    requestPs.setInt(i + 1, i);
                }
            } else if (columnType == Types.BIGINT) {
                if (table.getIndex().contains(i)) {
                    requestPs.setLong(i + 1, pkBase + i);
                } else {
                    requestPs.setLong(i + 1, i);
                }
            } else if (columnType == Types.TIMESTAMP) {
                requestPs.setTimestamp(i + 1, new Timestamp(System.currentTimeMillis()));
            } else if (columnType == Types.DATE) {
                requestPs.setDate(i + 1, new Date(System.currentTimeMillis()));
            }
        }
        return  requestPs;
    }

    @Setup
    public void setup() throws SQLException {
        init();
        if (!executor.createDB(db)) {
            return;
        }
        for (TableInfo table : tableMap.values()) {
            if (!executor.executeDDL(db, table.getDDL())) {
                return;
            }
        }
        putData();
    }

    @TearDown
    public void teardown() throws SQLException {
        for (String name : tableMap.keySet()) {
            executor.executeDDL(db, "drop table " + name + ";");
        }
        executor.dropDB(db);
    }

    @Benchmark
    public void execSQL() {
        try {
            PreparedStatement ps = getPreparedStatement();
            ResultSet resultSet = ps.executeQuery();
            /*resultSet.next();
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, String> val = new HashMap<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i + 1);
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    val.put(columnName, String.valueOf(resultSet.getString(i + 1)));
                } else if (columnType == Types.DOUBLE) {
                    val.put(columnName, String.valueOf(resultSet.getDouble(i + 1)));
                } else if (columnType == Types.INTEGER) {
                    val.put(columnName, String.valueOf(resultSet.getInt(i + 1)));
                } else if (columnType == Types.BIGINT) {
                    val.put(columnName, String.valueOf(resultSet.getLong(i + 1)));
                } else if (columnType == Types.TIMESTAMP) {
                    val.put(columnName, String.valueOf(resultSet.getTimestamp(i + 1)));
                } else if (columnType == Types.DATE) {
                    val.put(columnName, String.valueOf(resultSet.getDate(i+ 1)));
                }
            }*/
            ps.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws RunnerException {
      /*FESQLFZBenchmark ben = new FESQLFZBenchmark();
      try {
          ben.setup();
          ben.execSQL();
          ben.teardown();
      } catch (Exception e) {
          e.printStackTrace();
      }*/
        Options opt = new OptionsBuilder()
                .include(FESQLFZBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}

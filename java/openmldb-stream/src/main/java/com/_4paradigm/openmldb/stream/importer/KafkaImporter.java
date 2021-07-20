package com._4paradigm.openmldb.stream.importer;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class KafkaImporter {

    public static void main(String[] argv) throws Exception {
        int checkpointInterval = 10000;
        String jobName = "KafkaImporter";
        String tableSchema = "id STRING, vendor_id INT";

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        sEnv.enableCheckpointing(checkpointInterval);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, bsSettings);

        

        String sourceSql = "CREATE TABLE kafkaTable (\n" +
                " %s\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'taxi_tour6',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        String.format(sourceSql, tableSchema);
        tEnv.sqlUpdate(sourceSql);

        String createCatalogSql = "CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs://172.27.128.215/user/tobe/iceberg_demo5/',\n" +
                "  'property-version'='1'\n" +
                ")";
        tEnv.sqlUpdate(createCatalogSql);

        String insertSql = "INSERT INTO hadoop_catalog.taxi_tour.two_columns SELECT id, vendor_id FROM kafkaTable";
        tEnv.sqlUpdate(insertSql);

        tEnv.execute(jobName);

    }

}

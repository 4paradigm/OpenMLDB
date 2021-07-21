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
        String kafkaTopic = "taxi_tour6";
        String kafkaServers = "localhost:9092";
        String kafkaGroupId = "testGroup";
        String icebergCatalogName = "hadoop_catalog";
        String icebergWarehousePath = "hdfs://172.27.128.215/user/tobe/iceberg_demo5/";
        String icebergDatabaseName = "taxi_tour";
        String icebergTableName = "two_columns";


        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        sEnv.enableCheckpointing(checkpointInterval);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, bsSettings);


        String sourceSql = "CREATE TABLE kafkaTable (\n" +
                String.format(" %s\n", tableSchema) +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                String.format(" 'topic' = '%s',\n", kafkaTopic) +
                String.format(" 'properties.bootstrap.servers' = '%s',\n", kafkaServers) +
                String.format(" 'properties.group.id' = '%s',\n", kafkaGroupId) +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        tEnv.sqlUpdate(sourceSql);

        String createCatalogSql = String.format("CREATE CATALOG %s WITH (\n", icebergCatalogName) +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                String.format("  'warehouse'='%s',\n", icebergWarehousePath) +
                "  'property-version'='1'\n" +
                ")";
        tEnv.sqlUpdate(createCatalogSql);

        String insertSql = String.format("INSERT INTO %s.%s.%s SELECT * FROM kafkaTable", icebergCatalogName,
                icebergDatabaseName, icebergTableName);
        tEnv.sqlUpdate(insertSql);

        tEnv.execute(jobName);

    }

}

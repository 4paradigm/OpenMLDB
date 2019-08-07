package com._4paradigm.rtidbCmdUtil;


import com._4paradigm.rtidb.common.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Scanner;

public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);
    private static String zkEndpoints;  // 配置zk地址, 和集群启动配置中的zk_cluster保持一致
    private static String zkRootPath;   // 配置集群的zk根路径, 和集群启动配置中的zk_root_path保持一致

    public static void main(String[] args) {
        String zkEndpoints = args[0];
        String zkRootPath = args[1];
        System.out.println("Initialization starting...");
        RtidbClient rtidbClient = new RtidbClient(zkEndpoints, zkRootPath);
        System.out.println("Initialization completed");
        Scanner scanner = new Scanner(System.in);
        System.out.print("> ");
        while (scanner.hasNext()) {
            String input = scanner.nextLine();
            String[] arr = input.split("\\s+");
            String firstStr = arr[0].trim();
            if (firstStr.equals("create")) {
                if (arr.length == 2) {
                    if (new File(arr[1]).exists()) {
                        rtidbClient.createSchemaTableFromFile(arr[1]);
                    } else {
                        System.out.println("file " + arr[1] + "did not exit");
                        System.out.println("create format error! ex: create table_meta_file | create name ttl partition_num replica_num [name:type:index ...]");
                    }
                } else if (arr.length < 5) {
                    System.out.println("create format error! ex: create table_meta_file | create name ttl partition_num replica_num [name:type:index ...]");
                } else if (arr.length == 5) {
                    rtidbClient.createKVTable(arr[1], arr[2], arr[3], arr[4]);
                } else {
                    rtidbClient.createSchemaTable(arr);
                }
            } else if (firstStr.equals("put")) {
                if (arr.length < 3) {
                    System.out.println("put format error. eg: put table_name pk ts value | put table_name [ts] field1 field2 ...");
                } else if ((rtidbClient.getSchema(arr[1]) == null || rtidbClient.getSchema(arr[1]).size() == 0) && arr.length == 5) {
                    rtidbClient.putKv(arr[1], arr[2], arr[3], arr[4]);
                } else {
                    rtidbClient.putSchema(arr);
                }
            } else if (firstStr.equals("get")) {
                if (arr.length < 4) {
                    System.out.println("get format error. eg: get table_name key ts | get table_name key idx_name ts |  get table_name key1|key2... idxname time tsName");
                } else if ((rtidbClient.getSchema(arr[1]) == null || rtidbClient.getSchema(arr[1]).size() == 0) && arr.length == 4) {
                    rtidbClient.getKvData(arr[1], arr[2], arr[3]);
                } else {
                    rtidbClient.getSchemaData(arr);
                }
            } else if (firstStr.equals("showschema")) {
                if (arr.length != 2) {
                    System.out.println("showschema format error. eg: showschema tablename");
                } else{
                    List<Common.ColumnDesc> schema = rtidbClient.getSchema(arr[1]);
                    if (schema == null || schema.size() == 0) {
                        System.out.println("table " + arr[1] + " has not schema");
                    } else {
                        System.out.println("#ColumnDesc");
                        System.out.println(schema + "\n");
                    }
                    List<Common.ColumnKey> columnkey = rtidbClient.getColumnkey(arr[1]);
                    if (columnkey == null || columnkey.size() == 0) {
                        System.out.println("table " + arr[1] + " has not columnkey");
                    } else {
                        System.out.println("#ColumnKey");
                        System.out.println(columnkey);
                    }
                }
            } else if (firstStr.equals("preview")) {
                if (arr.length != 2) {
                    System.out.println("preview format error. eg: preview table_name");
                } else if ((rtidbClient.getSchema(arr[1]) == null || rtidbClient.getSchema(arr[1]).size() == 0)) {
                    rtidbClient.previewKv(arr[1]);
                } else {
                    rtidbClient.previewSchema(arr[1]);
                }
            } else if (firstStr.equals("scan")) {
                if (arr.length < 5) {
                    System.out.println("scan format error. eg: scan table_name pk start_time end_time [limit] | scan table_name key key_name start_time end_time [limit] | scan table_name key1|key2.. col_name start_time end_time tsName [limit]");
                } else if ((rtidbClient.getSchema(arr[1]) == null || rtidbClient.getSchema(arr[1]).size() == 0)) {
                    rtidbClient.scanKv(arr);
                } else {
                    rtidbClient.scanSchema(arr);
                }
            } else if (firstStr.equals("exit") || firstStr.equals("quit")) {
                System.out.println("bye");
                System.exit(0);
            } else
                System.out.println("unsupported cmd");
            System.out.print("> ");
        }
    }
}

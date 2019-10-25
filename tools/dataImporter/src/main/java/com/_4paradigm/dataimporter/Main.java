package com._4paradigm.dataimporter;

import com._4paradigm.dataimporter.initialization.*;
import com._4paradigm.dataimporter.parseutil.ParseCsvUtil;
import com._4paradigm.dataimporter.parseutil.ParseOrcUtil;
import com._4paradigm.dataimporter.parseutil.ParseParquetUtil;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(InitClient.class);
    private static final String FILEPATH;
    private static final String TABLENAME;
    private static boolean table_exists;

    static {
        InitAll.init();
        table_exists = Boolean.parseBoolean(
                StringUtils.isBlank(Constant.TABLE_EXIST) ? "false" : Constant.TABLE_EXIST);
        FILEPATH = Constant.FILEPATH;
        TABLENAME = Constant.TABLENAME;
    }

    private static void putFile(File file) {
        if (!table_exists) {
            if (!createTable(file)) {
                logger.error("creating table failed");
                return;
            }
            table_exists = true;
        }
        put(file.toPath().toString());
    }

    private static void putDirectory(File rootFile) {
        File[] files = rootFile.listFiles();
        if (files == null) {
            logger.error("there is no file in the directory " + rootFile);
            return;
        }
        List<java.nio.file.Path> filePaths = new ArrayList<>();
        for (File file : files) {
            logger.info("file path is : " + file.toPath().toString());
            if (file.toPath().toString().contains("parquet")
                    || file.toPath().toString().contains("csv")
                    || file.toPath().toString().contains("orc")) {
                if (!table_exists) {
                    if (!createTable(file)) {
                        logger.error("creating table failed");
                        return;
                    }
                    table_exists = true;
                }
                filePaths.add(file.toPath());
            }
        }
        for (java.nio.file.Path filePath : filePaths) {
            put(filePath.toString());
        }
    }

    private static boolean createTable(File file) {
        List<ColumnDesc> schemaList = null;
        String filePath = file.toPath().toString();
        if (filePath.contains("parquet")) {
            MessageType schema = ParseParquetUtil.getSchema(new Path(filePath));
            if (schema == null) {
                logger.error("the schema is null");
                return false;
            }
            schemaList = ParseParquetUtil.getSchemaOfRtidb(schema);
        } else if (filePath.contains("orc")) {
            TypeDescription schema = ParseOrcUtil.getSchema(filePath);
            if (schema == null) {
                logger.error("the schema is null");
                return false;
            }
            schemaList = ParseOrcUtil.getSchemaOfRtidb(schema);
        } else if (filePath.contains("csv")) {
            List<String[]> schema = ParseCsvUtil.getSchema(InitProperties.getProperties().getProperty("csv.schemaPath"));
            if (schema == null) {
                logger.error("the schema is null");
                return false;
            }
            schemaList = ParseCsvUtil.getSchemaOfRtidb(schema);
        }
        if (schemaList == null) {
            logger.error("the schemaList is null");
            return false;
        }
        if (Constant.STORAGEMODE.equals("ssd")) {
            return InitClient.createSchemaTable(Common.StorageMode.kSSD, Constant.TABLENAME, schemaList);
        } else if (Constant.STORAGEMODE.equals("hdd")) {
            return InitClient.createSchemaTable(Common.StorageMode.kHDD, Constant.TABLENAME, schemaList);
        } else {
            return InitClient.createSchemaTable(Common.StorageMode.kMemory, Constant.TABLENAME, schemaList);
        }
    }

    private static void put(String filePath) {
        if (filePath.contains("parquet")) {
            MessageType schema = ParseParquetUtil.getSchema(new Path(filePath));
            if (schema == null) {
                logger.error("the schema is null");
                return;
            }
            ParseParquetUtil parseParquetUtil = new ParseParquetUtil(filePath, TABLENAME, schema);
            parseParquetUtil.put();
            return;
        }
        if (filePath.contains("orc")) {
            TypeDescription schema = ParseOrcUtil.getSchema(filePath);
            if (schema == null) {
                logger.error("the schema is null");
                return;
            }
            ParseOrcUtil parseOrcUtil = new ParseOrcUtil(filePath, TABLENAME, schema);
            parseOrcUtil.put();
            return;
        }
        if (filePath.contains("csv")) {
            List<String[]> schema = ParseCsvUtil.getSchema(InitProperties.getProperties().getProperty("csv.schemaPath"));
            if (schema == null) {
                logger.error("the schema is null");
                return;
            }
            ParseCsvUtil parseCsvUtil = new ParseCsvUtil(filePath, TABLENAME, schema);
            parseCsvUtil.put();
        }
    }


    private static void shutDownThreadPoolExecutor() {
        InitThreadPool.getExecutor().shutdown();
        while (!InitThreadPool.getExecutor().isTerminated()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("The successfully number of inserted is : " + PutTask.successfulCount);
        logger.info("The failed number of inserted is : " + PutTask.failedCount);
    }

    public static void main(String[] args) {
        File rootFile = new File(FILEPATH);
        if (!rootFile.exists()) {
            logger.error("the rootFile does not exist");
            return;
        }
        if (rootFile.isDirectory()) {
            putDirectory(rootFile);
        } else if (rootFile.isFile()) {
            putFile(rootFile);
        }
        shutDownThreadPoolExecutor();
    }
}

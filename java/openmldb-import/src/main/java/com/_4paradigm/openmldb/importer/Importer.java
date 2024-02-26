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

package com._4paradigm.openmldb.importer;

import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Tablet;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;

@CommandLine.Command(name = "Data Importer", mixinStandardHelpOptions = true, requiredOptionMarker = '*',
        description = "insert/bulk load data(csv) to openmldb")
public class Importer {
    private static final Logger logger = LoggerFactory.getLogger(Importer.class);

    enum Mode {
        Insert,
        BulkLoad
    }

    @CommandLine.Option(names = "--importer_mode", description = "mode: ${COMPLETION-CANDIDATES}. Case insensitive.", defaultValue = "BulkLoad")
    Mode mode;

    @CommandLine.Option(names = "--files", description = "sources files, local or hdfs path", split = ",", required = true)
    private List<String> files;

    @CommandLine.Option(names = {"--zk_cluster", "-z"}, description = "zookeeper cluster address of openmldb", required = true)
    private String zkCluster;
    @CommandLine.Option(names = "--zk_root_path", description = "zookeeper root path of openmldb", required = true)
    private String zkRootPath;
    @CommandLine.Option(names = "--db", description = "openmldb database", required = true)
    private String dbName;
    @CommandLine.Option(names = "--table", description = "openmldb table", required = true)
    private String tableName;

    @CommandLine.Option(names = "--create_ddl", description = "if table is not exists or force_recreate_table is true, provide the create table sql", defaultValue = "")
    private String createDDL;
    @CommandLine.Option(names = {"-f", "--force_recreate_table"}, description = "if true, we will drop the table first")
    private boolean forceRecreateTable;

    public static final String rpcDataSizeMinLimit = "33554432"; // 32MB
    @CommandLine.Option(names = "--rpc_size_limit", description = "should >= " + rpcDataSizeMinLimit, defaultValue = rpcDataSizeMinLimit)
    private int rpcDataSizeLimit;

    @CommandLine.Option(names = "--rpc_write_timeout", description = "rpc write timeout(ms)", defaultValue = "10000")
    private int rpcWriteTimeout;
    @CommandLine.Option(names = "--rpc_read_timeout", description = "rpc read timeout(ms)", defaultValue = "50000")
    private int rpcReadTimeout;

    @CommandLine.Option(names = "--user", description = "the user to connect OpenMLDB", defaultValue = "root")
    private String user;

    @CommandLine.Option(names = "--password", description = "the password", defaultValue = "")
    private String password;

    FilesReader reader = null;
    SqlExecutor router = null;

    // src: file paths
    // read dir or *.xx?
    // how about SQL "LOAD DATA INFILE"? May need hdfs sasl config
    public boolean setUpSrcReader() {
        logger.info("files: {}", files);
        if (files.isEmpty()) {
            logger.info("config 'files' is empty");
            return false;
        }
        reader = new FilesReader(files);
        return true;
    }

    public boolean setUpSDK() {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkRootPath);
        option.setUser(user);
        option.setPassword(password);
        try {
            router = new SqlClusterExecutor(option);
            return true;
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        return false;
    }

    public boolean checkTable() {
        Preconditions.checkNotNull(router);
        if (forceRecreateTable && createDDL.isEmpty()) {
            logger.warn("recreate table needs create table ddl");
            return false;
        }

        if (forceRecreateTable) {
            logger.info("drop table {} for recreating", tableName);
            router.executeDDL(dbName, "drop table " + tableName + ";");
        }

        // if db is not existed, create it
        router.createDB(dbName);
        // create table
        router.executeDDL(dbName, createDDL);
        // TODO(hw): check table is empty
        return true;
    }

    public boolean checkRpcLimit() {
        int min = Integer.parseInt(rpcDataSizeMinLimit);
        return rpcDataSizeLimit >= min;
    }

    public void Load() {
        if (mode == Mode.Insert) {
            logger.error("insert mode needs refactor, unsupported now");
            return;
        }

        NS.TableInfo tableMetaData = getTableMetaData();
        if (tableMetaData == null) {
            logger.error("table {}.{} meta data is not found", dbName, tableName);
            return;
        }

        logger.debug(tableMetaData.toString());
        // TODO(hw): multi-threading insert into one MemTable? or threads num is less than MemTable size?
        logger.info("create generators for each table partition(MemTable)");
        Map<Integer, BulkLoadGenerator> generators = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        //  http/h2 can't add attachment, cuz it uses attachment to pass message. So we need to use brpc-java RpcClient
        List<RpcClient> rpcClients = new ArrayList<>();
        try {
            // When bulk loading, cannot AddIndex().
            //  And MemTable::table_index_ may be modified by AddIndex()/Delete...,
            //  so we should get table_index_'s info from MemTable, to know the real status.
            //  And the status can't be changed until bulk load finished.
            for (NS.TablePartition partition : tableMetaData.getTablePartitionList()) {
                logger.debug("tid-pid {}-{}, {}", tableMetaData.getTid(), partition.getPid(), partition.getPartitionMetaList());
                NS.PartitionMeta leader = partition.getPartitionMetaList().stream().filter(NS.PartitionMeta::getIsLeader).collect(onlyElement());

                RpcClientOptions clientOption = getRpcClientOptions();

                // Must list://
                String serviceUrl = "list://" + leader.getEndpoint();
                RpcClient rpcClient = new RpcClient(serviceUrl, clientOption);
                TabletService tabletService = BrpcProxy.getProxy(rpcClient, TabletService.class);
                Tablet.BulkLoadInfoRequest infoRequest = Tablet.BulkLoadInfoRequest.newBuilder().setTid(tableMetaData.getTid()).setPid(partition.getPid()).build();
                Tablet.BulkLoadInfoResponse bulkLoadInfo = tabletService.getBulkLoadInfo(infoRequest);
                logger.debug("get bulk load info of {} : {}", leader.getEndpoint(), bulkLoadInfo);

                // generate & send requests by BulkLoadGenerator
                // we need schema to parsing raw data in generator, got from NS.TableInfo
                BulkLoadGenerator generator = new BulkLoadGenerator(tableMetaData.getTid(), partition.getPid(), tableMetaData, bulkLoadInfo, tabletService, rpcDataSizeLimit);
                generators.put(partition.getPid(), generator);
                threads.add(new Thread(generator));

                // To close all clients
                rpcClients.add(rpcClient);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        logger.info("create {} generators, start bulk load...", generators.size());
        threads.forEach(Thread::start);

        // tableMetaData(TableInfo) to index map, needed by buildDimensions.
        // We build dimensions here, to distribute data to MemTables which should load the data.
        Preconditions.checkState(tableMetaData.getColumnKeyCount() > 0); // TODO(hw): no col key is available?

        Map<Integer, List<Integer>> keyIndexMap = new HashMap<>();
        Set<Integer> tsIdxSet = new HashSet<>();
        parseIndexMapAndTsSet(tableMetaData, keyIndexMap, tsIdxSet);

        // check header, header should == tableMetaData.
        reader.enableCheckHeader(tableMetaData);
        try {
            CSVRecord record;
            int lines = 0;
            while ((record = reader.next()) != null) {
                // integer gets added at the start of iteration for each line.
                lines++;
                Map<Integer, List<Tablet.Dimension>> dims = buildDimensions(record, keyIndexMap, tableMetaData.getPartitionNum());
                // distribute the row to the bulk load generators for each MemTable(tid, pid)
                for (Integer pid : dims.keySet()) {
                    // Note: NS pid is int
                    // no need to calc dims twice, pass it to BulkLoadGenerator
                    generators.get(pid).feed(new BulkLoadGenerator.FeedItem(dims, tsIdxSet, record));
                }
            }
            System.out.println("Total read rows: " + lines);
            // after while loop is finished, print out number of lines gone through.
        } catch (Exception e) {
            logger.error("feeding failed, {}", e.getMessage());
        }

        generators.forEach((integer, bulkLoadGenerator) -> bulkLoadGenerator.shutdownGracefully());
        logger.info("shutdown gracefully, waiting threads...");
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (generators.values().stream().anyMatch(BulkLoadGenerator::hasInternalError)) {
            System.out.println("bulk load failed, reloading needs to drop this table");
        } else {
            System.out.println("bulk load succeed");
        }

        // TODO(hw): get statistics from generators
        // rpc client release
        rpcClients.forEach(RpcClient::stop);
    }

    private RpcClientOptions getRpcClientOptions() {
        RpcClientOptions clientOption = new RpcClientOptions();
        clientOption.setWriteTimeoutMillis(rpcWriteTimeout); // concurrent rpc may let write slowly
        clientOption.setReadTimeoutMillis(rpcReadTimeout); // index rpc may take time, cuz need to do bulk load
        // clientOption.setMinIdleConnections(10);
        // clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        clientOption.setGlobalThreadPoolSharing(true);
        // TODO(hw): disable retry for simplicity, timeout retry is dangerous
        clientOption.setMaxTryTimes(1);
        return clientOption;
    }

    private NS.TableInfo getTableMetaData() {
        try {
            logger.info("query zk for table {} meta data", tableName);
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient(zkCluster, retryPolicy);
            client.start();
            String tableInfoPath = zkRootPath + "/table/db_table_data";
            List<String> tables = client.getChildren().forPath(tableInfoPath);
            Preconditions.checkNotNull(tables, "zk path {} get child failed.", tableInfoPath);

            for (String tableId : tables) {
                byte[] tableInfo = client.getData().forPath(tableInfoPath + "/" + tableId);
                NS.TableInfo info = NS.TableInfo.parseFrom(tableInfo);
                if (info.getName().equals(tableName) && info.getDb().equals(dbName)) {
                    return info;
                }
            }
            client.close();
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        return null;
    }

    public void close() {
        router.close();
    }

    public static void main(String[] args) {
        Importer importer = new Importer();
        CommandLine cmd = new CommandLine(importer).setCaseInsensitiveEnumValuesAllowed(true);
        try {
            URL prop = importer.getClass().getClassLoader().getResource("importer.properties");
            logger.info("load properties file {}", Objects.requireNonNull(prop).getFile());
            File defaultsFile = new File(prop.getFile());
            cmd.setDefaultValueProvider(new CommandLine.PropertiesDefaultProvider(defaultsFile));
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("can't load properties file");
        }
        // properties can be overwritten by command line
        cmd.parseArgs(args);
        if (cmd.isUsageHelpRequested()) {
            CommandLine.usage(cmd, System.out);
            return;
        }

        logger.info("Start...");

        if (!importer.setUpSrcReader()) {
            logger.error("set up src reader failed");
            return;
        }
        if (!importer.setUpSDK()) {
            logger.error("set up sdk failed");
            return;
        }
        if (!importer.checkTable()) {
            logger.error("check table failed");
            return;
        }

        if (!importer.checkRpcLimit()) {
            logger.error("rpc limit should > {}", Importer.rpcDataSizeMinLimit);
            return;
        }

        System.out.println("start bulk load");
        long startTime = System.currentTimeMillis();
        importer.Load();
        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;
        System.out.println("End. Total time: " + totalTime + " ms");

        importer.close();
    }

    // TODO(hw): insert import mode refactor. limited retry, report the real-time status of progress.
    // can't find a good import tool framework, may ref mysqlimport
    private static void insertImportByRange(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName) {
        int quotient = rows.size() / X;
        int remainder = rows.size() % X;
        // range is [left, right)
        List<Pair<Integer, Integer>> ranges = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < remainder; ++i) {
            int rangeEnd = Math.min(start + quotient + 1, rows.size());
            ranges.add(Pair.of(start, rangeEnd));
            start = rangeEnd;
        }
        for (int i = remainder; i < X; ++i) {
            int rangeEnd = Math.min(start + quotient, rows.size());
            ranges.add(Pair.of(start, rangeEnd));
            start = rangeEnd;
        }

        List<Thread> threads = new ArrayList<>();
        for (Pair<Integer, Integer> range : ranges) {
            threads.add(new Thread(new InsertImporter(router, dbName, tableName, rows, range)));
        }

        threads.forEach(Thread::start);

        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        });

    }

    private static void parseIndexMapAndTsSet(NS.TableInfo table, Map<Integer, List<Integer>> keyIndexMap, Set<Integer> tsIdxSet) {
        Map<String, Integer> nameToIdx = new HashMap<>();
        for (int i = 0; i < table.getColumnDescCount(); i++) {
            nameToIdx.put(table.getColumnDesc(i).getName(), i);
        }
        for (int i = 0; i < table.getColumnKeyCount(); i++) {
            Common.ColumnKey key = table.getColumnKey(i);
            for (String colName : key.getColNameList()) {
                List<Integer> keyCols = keyIndexMap.getOrDefault(i, new ArrayList<>());
                keyCols.add(nameToIdx.get(colName));
                keyIndexMap.put(i, keyCols);
            }
            if (key.hasTsName()) {
                tsIdxSet.add(nameToIdx.get(key.getTsName()));
            }
        }
    }

    // ref SQLInsertRow::GetDimensions()
    // TODO(hw): integer or long?
    private static Map<Integer, List<Tablet.Dimension>> buildDimensions(CSVRecord record, Map<Integer, List<Integer>> keyIndexMap, int pidNum) {
        Map<Integer, List<Tablet.Dimension>> dims = new HashMap<>();
        int pid = 0;
        for (Map.Entry<Integer, List<Integer>> entry : keyIndexMap.entrySet()) {
            Integer index = entry.getKey();
            List<Integer> keyCols = entry.getValue();
            String combinedKey = keyCols.stream().map(record::get).collect(Collectors.joining("|"));
            if (pidNum > 0) {
                pid = (int) Math.abs(MurmurHash.hash64(combinedKey) % pidNum);
            }
            List<Tablet.Dimension> dim = dims.getOrDefault(pid, new ArrayList<>());
            dim.add(Tablet.Dimension.newBuilder().setKey(combinedKey).setIdx(index).build());
            dims.put(pid, dim);
        }
        return dims;
    }

    private static String printDimensions(Map<Integer, List<Tablet.Dimension>> dims) {
        return dims.entrySet().stream().map(entry -> entry.getKey().toString() + ": " +
                        entry.getValue().stream().map(pair ->
                                        "<" + pair.getKey() + ", " + pair.getIdx() + ">")
                                .collect(Collectors.joining(", ", "(", ")")))
                .collect(Collectors.joining("], [", "[", "]"));
    }
}


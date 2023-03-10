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

package com._4paradigm.openmldb.synctool;

import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.DataSync;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Common.ColumnDesc;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.common.zk.ZKConfig;
import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.math.NumberUtils;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import com.google.protobuf.TextFormat;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.collect.MoreCollectors.onlyElement;

@Slf4j
public class SyncToolImpl implements SyncToolInterface {
    private final String endpoint;
    private volatile ZKClient zkClient;
    private SqlExecutor router;
    private String zkCollectorPath;
    private ScheduledExecutorService executor;

    // holds all sync tasks <tid, <pid, SyncTask>>, big lock
    // guard by itself
    private Map<Integer, Map<Integer, SyncTask>> syncTasks = Collections.synchronizedMap(new HashMap<>());
    // <tid, <db, table>>, we need to know db and table name for each tid to get
    // table info(to find data collector, to get the latest schema)
    private Map<Integer, Pair<String, String>> tid2dbtable = Collections.synchronizedMap(new HashMap<>());

    public SyncToolImpl(String endpoint) throws SqlException, InterruptedException {
        this.endpoint = endpoint;
        this.zkClient = new ZKClient(ZKConfig.builder()
                .cluster(SyncToolConfig.ZK_CLUSTER)
                .namespace(SyncToolConfig.ZK_ROOT_PATH)
                .build());
        Preconditions.checkState(zkClient.connect(), "zk connect failed");
        SdkOption option = new SdkOption();
        option.setZkCluster(SyncToolConfig.ZK_CLUSTER);
        option.setZkPath(SyncToolConfig.ZK_ROOT_PATH);
        this.router = new SqlClusterExecutor(option);
        this.zkCollectorPath = SyncToolConfig.ZK_ROOT_PATH + "/sync_tool/collector";

        // a background thread to check sync task status and reassign tasks
        executor = Executors.newScheduledThreadPool(1);
    }

    public void init() {
        // setup tunnel env first
        // hdfs tunnel hasn't cache, just write to hdfs
        Preconditions.checkNotNull(HDFSTunnel.getInstance());
        // sleep?
        // recover sync tasks from file
        recover();

        // start task check bg thread, don't start before recover(may set task to
        // unalive too fast), so delay 5s
        int initialDelay = 5;
        int period = SyncToolConfig.TASK_CHECK_PERIOD;
        TimeUnit unit = TimeUnit.SECONDS;
        // for check thread
        long periodMs = unit.toMillis(period);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Runnable taskCheck = () -> {
            log.debug("check sync task status");
            // for each task
            List<SyncTask> reassignTasks = new ArrayList<>();
            synchronized (syncTasks) {
                for (Map.Entry<Integer, Map<Integer, SyncTask>> entry : syncTasks.entrySet()) {
                    int tid = entry.getKey();
                    Map<Integer, SyncTask> tasksForTid = entry.getValue();
                    // check if all in SUCCESS status
                    if (tasksForTid.values().stream().allMatch(
                            t -> t.getStatus() == SyncTask.Status.SUCCESS)) {
                        log.info("tid {} sync task all SUCCESS, remove it", tid);
                        cleanEnv(tid);
                        syncTasks.remove(tid);
                    }
                    for (Map.Entry<Integer, SyncTask> entry2 : tasksForTid.entrySet()) {
                        int pid = entry2.getKey();
                        SyncTask task = entry2.getValue();
                        // check sync task update time, ignore unalive tasks
                        synchronized (task) {
                            if (task.getStatus() == SyncTask.Status.RUNNING
                                    && task.getLastUpdateTime().get() + periodMs * 2 < System.currentTimeMillis()) {
                                // task timeout, reassign
                                log.info("task timeout, reassign, {}, last update time {}", task.extraInfo(),
                                        formatter.format(new Date(task.getLastUpdateTime().get())));
                                // mark task as reassigning to avoid send data to it
                                task.setStatus(SyncTask.Status.REASSIGNING);
                                reassignTasks.add(task);
                            }
                        }
                        // TODO if flink tunnel, check count with flink sink metric? to know if mode 0
                        // task is finished
                    }
                }
            }
            // reassign tasks outside the lock
            for (SyncTask task : reassignTasks) {
                // choose another data collector if the current one is down or partition leader
                // changed, just use the current living one
                synchronized (task) {
                    try {
                        Pair<String, String> pair = findDataCollectorInHost(task.getProgress().getTid(),
                                task.getProgress().getPid());
                        task.setTabletServer(pair.getLeft());
                        task.setDataCollector(pair.getRight());
                        // new token
                        task.setToken(genToken());
                        addTaskInDataCollector(task);
                        // a longer timeout
                        task.getLastUpdateTime().set(period * 2 + System.currentTimeMillis());
                        // enable SendData
                        task.setStatus(SyncTask.Status.RUNNING);
                    } catch (Exception e) {
                        log.error("reassign task in data collector failed, task: {}, no longer handle it",
                                task.extraInfo(), e);
                        task.setStatus(SyncTask.Status.FAILED);
                    }
                }
            }
        };

        executor.scheduleAtFixedRate(taskCheck, initialDelay, period, unit);
    }

    // not thread safe, call before serving. If recover failed, don't start serving
    private void recover() {
        Path progressPathRoot = Paths.get(SyncToolConfig.SYNC_TASK_PROGRESS_PATH);
        if (Files.notExists(progressPathRoot)) {
            log.info("no sync task progress file, skip recover");
            return;
        }
        // read all sync tasks' progress from file, mark it as alive, bg thread will
        // reassgin it if timeout
        // <tid, sinkPath
        Map<Integer, String> tids = new HashMap<>();
        try (Stream<Path> stream = Files.walk(progressPathRoot)) {
            List<Path> bakFiles = stream
                    .filter(Files::isRegularFile)
                    // skip bak files and bak tid dir
                    .filter(file -> file.getFileName().toString().endsWith(".progress")
                            && NumberUtils.isParsable(file.getParent().getFileName().toString()))
                    .collect(Collectors.toList());
            log.info("try to recover {} sync task progress", bakFiles.size());
            // repartition it
            Map<Integer, List<Path>> tid2Files = new HashMap<>();
            for (Path file : bakFiles) {
                // <tid>/<pid>.progress
                int tid = Integer.parseInt(file.getParent().getFileName().toString());
                // int pid = Integer.parseInt(file.getFileName().toString().split("\\.")[0]);
                List<Path> files = tid2Files.get(tid);
                if (files == null) {
                    files = new ArrayList<>();
                    tid2Files.put(tid, files);
                }
                files.add(file);
            }
            log.info("find {} table sync task, detail {}", tid2Files.size(), tid2Files);
            String sinkPath = null;
            for (Map.Entry<Integer, List<Path>> entry : tid2Files.entrySet()) {
                int tid = entry.getKey();
                Preconditions.checkState(
                        !syncTasks.containsKey(tid), "tid already in syncTasks, sth wrong, tid: " + tid);
                syncTasks.put(tid, new HashMap<>());
                // recover all pid task
                log.info("recover sync task for tid: {}, pid size {}", tid, entry.getValue().size());
                for (Path file : entry.getValue()) {
                    log.info("recover sync task progress, file: {}", file);
                    SyncTask task = SyncTask.recover(file.toAbsolutePath().toString());
                    log.info("extracted task: {}", task.buildTaskPB());
                    DataSync.AddSyncTaskRequest progress = task.getProgress();
                    Preconditions.checkState(tid == progress.getTid(), "tid not match, file: %s", file);
                    // TODO(hw): check pid consecutive?
                    int pid = progress.getPid();
                    // add task to syncTasks
                    Map<Integer, SyncTask> tasksForTid = Preconditions.checkNotNull(syncTasks.get(tid));
                    // we don't assign task to data collector, just add it to syncTasks, let bg
                    // thread to reassgin it, so we should set running here
                    task.setStatus(SyncTask.Status.RUNNING);
                    tasksForTid.put(pid, task);
                    if (sinkPath == null) {
                        sinkPath = progress.getDest();
                    } else {
                        Preconditions.checkState(sinkPath.equals(progress.getDest()),
                                "sink path not match, file: %s, sinkPath: %s", file, sinkPath);
                    }
                }

                Preconditions.checkState(!tids.containsKey(tid),
                        "tid already in tids, sth wrong, tid: " + tid + ", sinkPath: " + sinkPath);
                tids.put(tid, Preconditions.checkNotNull(sinkPath));
                // recover tunnel env
                try {
                    Preconditions.checkState(HDFSTunnel.getInstance().recoverTunnel(tid, sinkPath));
                } catch (Exception e) {
                    log.error("recover tunnel env failed, tid: {}, remove the env", tid, e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "read sync task progress dir failed, dir: " + SyncToolConfig.SYNC_TASK_PROGRESS_PATH, e);
        }

        // get db and table name for each tid
        try {
            Statement stmt = router.getStatement();
            Preconditions.checkState(stmt.execute("show table status"));
            ResultSet rs = stmt.getResultSet();
            // can't use name, use idx to get
            while (rs.next()) {
                // Table_id
                int tid = Integer.parseInt(rs.getString(1));
                if (!tids.containsKey(tid)) {
                    continue;
                }
                // Table_name 2, Database_name 3
                String db = rs.getString(3);
                String table = rs.getString(2);
                tid2dbtable.put(tid, Pair.of(db, table));
            }
            // if the disappeared table will be recovered later, how to handle it? So leave
            // the progress files, don't delete them
            Preconditions.checkState(tids.size() == tid2dbtable.size(),
                    "some tables are disappeared, tids: %s, tid2dbtable: %s", tids, tid2dbtable);
        } catch (SQLException e) {
            throw new RuntimeException("recover tid2dbtable failed", e);
        }
        log.info("recover {} sync tasks, syncTasks: {}, tid2dbtable: {}", syncTasks.size(), syncTasks,
                tid2dbtable);
    }

    @Override
    public DataSync.GeneralResponse CreateSyncTask(DataSync.CreateSyncTaskRequest request) {
        log.info("Receive CreateSyncTask request: {}", request);
        // create sync task env
        DataSync.GeneralResponse.Builder respBuilder = DataSync.GeneralResponse.newBuilder().setCode(0);
        // get partition info of table getTableInfo
        NS.TableInfo tableInfo;
        int tid = -1;
        try {
            tableInfo = router.getTableInfo(request.getDb(), request.getName());
            log.debug("tableInfo: {}", tableInfo);
            // check storage mode
            Preconditions.checkArgument(tableInfo.getStorageMode() == Common.StorageMode.kSSD
                    || tableInfo.getStorageMode() == Common.StorageMode.kHDD,
                    "only SSD and HDD are supported");
            tid = tableInfo.getTid();

            // use creatingTasks to setup for one table
            synchronized (syncTasks) {
                if (syncTasks.containsKey(tid)) {
                    // just return, do not do clean
                    respBuilder.setCode(-1).setMsg("sync task already exists(creating or created)");
                    return respBuilder.build();
                }
                syncTasks.put(tid, new HashMap<>());
                log.info(
                        "creating sync tasks for table {}.{}({})", request.getDb(), request.getName(), tid);
            }

            // do not lock the whole part cuz creating tasks on data collector may update
            // the task progress
            for (NS.TablePartition partitionInfo : tableInfo.getTablePartitionList()) {
                log.debug("partitionInfo: {}", partitionInfo);
                Pair<String, String> pair = findDataCollectorInHost(partitionInfo);
                String tabletServer = pair.getLeft();
                String dataCollector = pair.getRight();
                // add sync task
                int pid = partitionInfo.getPid();
                SyncTask task = createSyncTask(tid, pid, request, tabletServer, dataCollector);
                syncTasks.get(tid).put(pid, task);
                // add sync task to data collector, anyone failed, reject all
                addTaskInDataCollector(task);
            }
            tid2dbtable.put(tid, Pair.of(request.getDb(), request.getName()));

            Preconditions.checkState(HDFSTunnel.getInstance().createTunnel(tid, "", request.getDest()),
                    "create hdfs tunnel failed");
            log.info("add sync tasks for table {}.{}({}[{}]), sync tasks: {}", request.getDb(),
                    request.getName(), tid, syncTasks.get(tid).size(), syncTasks);
        } catch (Exception e) {
            e.printStackTrace();
            // just cleanup in sync tool, if DataCollector send data for not-exist task,
            // reject it
            cleanEnv(tid);
            respBuilder.setCode(-1).setMsg(e.toString());
        }
        return respBuilder.build();
    }

    /* cache methods used by flink */
    public static String genCacheDir(int tid) {
        return SyncToolConfig.DATA_CACHE_PATH + "/" + tid;
    }

    private void createCacheDir(int tid) throws IOException {
        java.nio.file.Path cachePath = Paths.get(genCacheDir(tid));
        Preconditions.checkState(
                Files.notExists(cachePath), "cache dir already exists, why? " + cachePath);
        Files.createDirectories(cachePath);
        Preconditions.checkState(Files.exists(cachePath) && Files.isDirectory(cachePath),
                "not exists or not a dir: " + cachePath);
    }

    private void deleteCacheDir(int tid) throws IOException {
        java.nio.file.Path cachePath = Paths.get(genCacheDir(tid));
        Preconditions.checkState(Files.exists(cachePath), "cache dir not exists, why? " + cachePath);
        FileUtils.deleteDirectory(cachePath.toFile());
        Preconditions.checkState(Files.notExists(cachePath), "delete cache dir failed, " + cachePath);
    }

    private void cleanEnv(int tid) {
        synchronized (syncTasks) {
            syncTasks.remove(tid);
            Path tidProcess = Paths.get(SyncToolConfig.SYNC_TASK_PROGRESS_PATH + '/' + tid);
            if (Files.exists(tidProcess)) {
                try {
                    Files.move(tidProcess,
                            Paths.get(SyncToolConfig.SYNC_TASK_PROGRESS_PATH + '/' + tid + ".deleted."
                                    + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                    log.warn("move sync task progress dir failed, dir: {}, continue.", tidProcess);
                }
            }
            // hdfs no cache
            HDFSTunnel.getInstance().closeTunnel(tid);
        }
    }

    private Pair<String, String> findDataCollectorInHost(int tid, int pid) throws Exception {
        Pair<String, String> pair = tid2dbtable.get(tid);
        Preconditions.checkState(pair != null, "can't find tid in tid2dbtable, tid: " + tid);
        NS.TableInfo tableInfo = router.getTableInfo(pair.getLeft(), pair.getRight());
        NS.TablePartition partitionInfo = tableInfo.getTablePartition(pid);
        Preconditions.checkState(partitionInfo.getPid() == pid,
                "partition id not match, partition list doesn't in order? tid: " + tid + ", pid: " + pid);
        log.debug("partitionInfo: {}", partitionInfo);
        return findDataCollectorInHost(partitionInfo);
    }

    private Pair<String, String> findDataCollectorInHost(NS.TablePartition partitionInfo) {
        NS.PartitionMeta leaderPartition = partitionInfo.getPartitionMetaList()
                .stream()
                .filter(partitionMeta -> {
                    return partitionMeta.getIsLeader();
                })
                .collect(onlyElement());
        log.debug("leaderPartition: {}", leaderPartition);
        // choose data collector in the same host to add sync task
        String tabletServer = leaderPartition.getEndpoint();
        String dataCollector = findDataCollectorInHost(tabletServer);
        return Pair.of(tabletServer, dataCollector);
    }

    // find data collector in the same host
    private String findDataCollectorInHost(String tabletServer) {
        String host = tabletServer.split(":")[0];
        Preconditions.checkState(!host.isEmpty(), "host is empty"); // or check host pattern?
        // full path in zk, get alive data collectors
        List<String> dataCollectors;
        try {
            Preconditions.checkState(zkClient.checkExists(zkCollectorPath));
            dataCollectors = zkClient.getChildren(zkCollectorPath);
        } catch (Exception e) {
            throw new RuntimeException("can't find data collectors in zk", e);
        }
        for (String dataCollector : dataCollectors) {
            String dataCollectorHost = dataCollector.split(":")[0];
            if (dataCollectorHost.equals(host)) {
                return dataCollector;
            }
        }
        throw new RuntimeException("can't find living data collector in host: " + host);
    }

    // gen unique token
    private synchronized String genToken() {
        return UUID.randomUUID().toString();
    }

    private SyncTask createSyncTask(int tid, int pid, DataSync.CreateSyncTaskRequest request,
            String tabletServer, String dataCollector) {
        Preconditions.checkState(request.hasMode(), "mode is required");
        DataSync.AddSyncTaskRequest.Builder addSyncTaskRequestBuilder = DataSync.AddSyncTaskRequest.newBuilder()
                .setTid(tid).setPid(pid).setMode(request.getMode());
        if (request.hasStartTs()) {
            addSyncTaskRequestBuilder.setStartTs(request.getStartTs());
        }
        // new sync point start from snapshot
        addSyncTaskRequestBuilder.getSyncPointBuilder().setType(DataSync.SyncType.SNAPSHOT);
        addSyncTaskRequestBuilder.setTabletEndpoint(tabletServer);
        addSyncTaskRequestBuilder.setDesEndpoint(this.endpoint); // set me
        addSyncTaskRequestBuilder.setToken(genToken());
        addSyncTaskRequestBuilder.setDest(request.getDest()); // for persist
        SyncTask task = new SyncTask(addSyncTaskRequestBuilder.build(), dataCollector);
        try {
            task.persist();
        } catch (IOException e) {
            throw new RuntimeException("persist new sync task failed", e);
        }
        return task;
    }

    // not thread safe
    private void addTaskInDataCollector(SyncTask task) {
        RpcClientOptions clientOption = new RpcClientOptions();
        // clientOption.setWriteTimeoutMillis(rpcWriteTimeout); // concurrent rpc may
        // let write slowly

        // rpc may take time, cuz data collector needs to create sync env
        clientOption.setReadTimeoutMillis((int) TimeUnit.SECONDS.toMillis(SyncToolConfig.REQUEST_TIMEOUT));
        // clientOption.setMinIdleConnections(10);
        // clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        clientOption.setGlobalThreadPoolSharing(true);
        // disable retry for simplicity, timeout retry is dangerous
        clientOption.setMaxTryTimes(1);
        // Must list://
        String serviceUrl = "list://" + task.getDataCollector();
        RpcClient rpcClient = new RpcClient(serviceUrl, clientOption);
        DataCollectorService dataCollectorService = BrpcProxy.getProxy(rpcClient, DataCollectorService.class);
        DataSync.GeneralResponse resp = dataCollectorService.addSyncTask(task.getProgress());
        if (resp.getCode() != 0) {
            throw new RuntimeException(
                    "add sync task failed, code: " + resp.getCode() + ", msg: " + resp.getMsg());
        }
    }

    @Override
    public DataSync.GeneralResponse DeleteSyncTask(DataSync.DeleteSyncTaskRequest request) {
        log.info("Receive DeleteSyncTask request: {}", request);
        DataSync.GeneralResponse.Builder responseBuilder = DataSync.GeneralResponse.newBuilder().setCode(0);
        // only delete sync task in me, not in data collector
        try {
            // db.name -> tid
            NS.TableInfo tableInfo = Preconditions
                    .checkNotNull(router.getTableInfo(request.getDb(), request.getName()));
            int tid = tableInfo.getTid();
            cleanEnv(tid);
        } catch (NullPointerException e) {
            responseBuilder.setCode(-1).setMsg(e.getMessage());
        } catch (Exception e) {
            responseBuilder.setCode(-1).setMsg("delete sync task failed");
        }

        return responseBuilder.build();
    }

    @Override
    public DataSync.SendDataResponse SendData(DataSync.SendDataRequest request) {
        log.info("Receive SendData request: {}", TextFormat.shortDebugString(request));
        DataSync.SendDataResponse.Builder respBuilder = DataSync.SendDataResponse.newBuilder();
        respBuilder.getResponseBuilder().setCode(0);
        try {
            SyncTask task;
            synchronized (syncTasks) {
                if (!syncTasks.containsKey(request.getTid())) {
                    // we may receive many invalid send data request, just log it, no need to print
                    // stack
                    throw new IgnorableException("can't find sync task, tid: " + request.getTid());
                }
                Map<Integer, SyncTask> tasksForTid = syncTasks.get(request.getTid());
                if (!tasksForTid.containsKey(request.getPid())) {
                    throw new IgnorableException(
                            "can't find sync task, tid: " + request.getTid() + ", pid: " + request.getPid());
                }
                task = tasksForTid.get(request.getPid());
            }
            synchronized (task) {
                // precheck, but still may fail cuz syncTasks table has created before tunnel
                // created, we can't avoid this(although we delay the first sync on data
                // collector, but it's not a guarantee)
                task.preCheck(request);
                // store data, store to file or else. data collector don't need to read them
                // again, so update the sync point
                if (request.getCount() != 0) {
                    ByteBuf data = RpcContext.getContext().getRequestBinaryAttachment();
                    Preconditions.checkState(data != null, "attachment data is null");
                    log.info("store data, tid: {}, pid: {}, count: {}, size: {}", request.getTid(),
                            request.getPid(), request.getCount(), data.readableBytes());
                    Pair<String, String> pair = tid2dbtable.get(request.getTid());
                    // get the newest schema, to avoid schema change
                    // not getTableSchema, because it's not good to RowView
                    NS.TableInfo tableInfo = router.getTableInfo(pair.getLeft(), pair.getRight());

                    // write to hdfs
                    HDFSTunnel.getInstance().writeData(
                            tableInfo.getTid(), data, request.getCount(), tableInfo.getColumnDescList());
                    // for flink, write to local file, flink monitor the file and read it
                    // saveToCache(genCacheDir(tableInfo.getTid()), data, request.getCount(),
                    // tableInfo.getColumnDescList());
                }
                // TODO(hw): what if sync tool shutdown here? already store data, but progress
                // is not updated

                // If store failed, don't update sync point, and no need to do other things.
                // Outdated tasks will be find by taskCheck.
                task.updateProgress(request);
            }

            if (request.hasFinished() && request.getFinished()) {
                // if data collector send finished, it's removed the task in itself already
                log.info("sync task {}-{} finished, bak and remove it", request.getTid(), request.getPid());
                task.close();
            }
        } catch (Exception e) {
            if (!(e instanceof IgnorableException)) {
                e.printStackTrace();
            }
            respBuilder.getResponseBuilder().setCode(-1).setMsg(e.toString());
            respBuilder.setDeleteTask(true);
            log.warn("Response error, let data collector delete the sync task, cuz {}", e.toString());
        }
        return respBuilder.build();
    }

    public void saveToCache(
            String cacheDir, ByteBuf data, final long count, List<ColumnDesc> schema) {
        Preconditions.checkArgument(!schema.isEmpty(), "schema is empty");
        Preconditions.checkArgument(!cacheDir.isEmpty(), "");

        // unpack data here, or in the row format?
        // ref ResultSetBase, write in java, don't make swig interface too complex
        try {
            // save to temp file, then rename to new file
            java.nio.file.Path tempFile = Files.createTempFile("cache-", ".tmp");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile.toFile()))) {
                DataParser parser = new DataParser(data, count, schema);
                parser.writeAll(writer);
            }
            java.nio.file.Path newFile = Paths.get(cacheDir + "/" + uniqueFileName());
            Files.move(tempFile, newFile);
            log.info("save to cache dir: {}, count: {}", newFile, count);
        } catch (Exception e) {
            throw new RuntimeException("save to cache dir failed", e);
        }
    }

    // only gen csv
    public synchronized static String uniqueFileName() {
        return UUID.randomUUID().toString() + ".csv";
    }

    @Override
    public DataSync.TaskStatusResponse TaskStatus(DataSync.TaskStatusRequest request) {
        log.info("Receive TaskStatus request: {}", request);
        DataSync.TaskStatusResponse.Builder respBuilder = DataSync.TaskStatusResponse.newBuilder();
        // default code 0
        respBuilder.getResponseBuilder().setCode(0);

        synchronized (syncTasks) {
            if (request.hasClearAll() && request.getClearAll()) {
                log.warn("clear all sync tasks and env");
                synchronized (syncTasks) {
                    syncTasks.keySet().forEach(tid -> cleanEnv(tid));
                    syncTasks.clear();
                }
            } else {
                // get all tasks
                for (Map.Entry<Integer, Map<Integer, SyncTask>> entry : syncTasks.entrySet()) {
                    Map<Integer, SyncTask> tasksForTid = entry.getValue();
                    for (Map.Entry<Integer, SyncTask> taskEntry : tasksForTid.entrySet()) {
                        SyncTask task = taskEntry.getValue();
                        respBuilder.addTask(task.buildTaskPB());
                        respBuilder.addReadableInfo(task.extraInfo());
                    }
                }
            }
        }

        return respBuilder.build();
    }
}

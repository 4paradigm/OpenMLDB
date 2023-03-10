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

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public class FlinkTunnel {

    private static FlinkTunnel instance;

    public static synchronized FlinkTunnel getInstance() {
        if (instance == null) {
            instance = new FlinkTunnel();
            try {
                SyncToolConfig.parse();
                instance.init(SyncToolConfig.getProp());
            } catch (Exception e) {
                e.printStackTrace();
                instance = null;
            }
        }
        return instance;
    }

    @Getter
    private MiniCluster miniCluster;
    private URI miniClusterAddr;
    private StreamExecutionEnvironment env;
    // <tid, jobID>
    private Map<Integer, JobID> sourceMap = new ConcurrentHashMap<>();

    private FlinkTunnel() {
    }

    private void init(Properties prop) throws Exception {
        Configuration configuration = Configuration.fromMap(Maps.fromProperties(prop));
        // ExecutionOptions
        configuration.setString("execution.runtime-mode", "STREAMING");
        // HighAvailabilityOptions ref zookeeper ha
        configuration.setString("high-availability", "zookeeper");
        configuration.setString("high-availability.zookeeper.quorum", SyncToolConfig.ZK_CLUSTER);

        // **Finished** ：流模式(`STREAMING`)下的成功的 Checkpoint 或者批模式(`BATCH`)下输入结束，文件的
        // Pending 状态转换为 Finished 状态
        MiniClusterConfiguration miniClusterConfig = new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(SyncToolConfig.FLINK_SLOTS) // can't run job num > slot?
                .build();

        miniCluster = new MiniCluster(miniClusterConfig);
        miniCluster.start();

        miniClusterAddr = miniCluster.getRestAddress().get();
        log.info("start a mini cluster, addr: {}", miniClusterAddr);

        env = StreamExecutionEnvironment.createRemoteEnvironment(
                miniClusterAddr.getHost(),
                miniClusterAddr.getPort(),
                configuration);
        // env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(5000); // ms
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // save the checkpoint to recover from SyncTool failures
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // one for one sync tool?
        checkpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints/");
        // setStateBackend?

        // recover jobs
        Collection<JobStatusMessage> oldJobs = miniCluster.listJobs().get();
        // old jobs start time will be updated
        // if running job no source bytes received, it will be canceled?
        // get Dataflow Plan by http or client

        // TODO how about watch a dir for a table? no append to old file, but create new
        // files
        // test about it
        // so job is unique for a table
        for (JobStatusMessage job : oldJobs) {
            if (!job.getJobState().isGloballyTerminalState()) {
                log.info("old job: {}", job);
                String name = job.getJobName();
                int tid = Integer.parseInt(name.split("-")[2]);
                java.nio.file.Path cachePath = Paths.get(SyncToolImpl.genCacheDir(tid));
                if (Files.notExists(cachePath)) {
                    log.warn("cache dir not exists(weird), create it: {}", cachePath);
                    Files.createDirectories(cachePath);
                }
                sourceMap.put(tid, job.getJobId());
            }
        }
        log.info("recovered jobs: {}", sourceMap);
    }

    public synchronized boolean recoverTunnel(int tid) {
        // already recovered when init, so just check here
        return sourceMap.containsKey(tid);
    }

    // synchronized to avoid create twice in the same time
    // init phase is quite slow, no need to do lock free
    public synchronized boolean createTunnel(int tid, String sourcePath, String sinkPath) {
        if (sourceMap.containsKey(tid)) {
            log.warn("tunnel for tid: {} already exists", tid);
            return false;
        }
        // for flink, we should add the prefix "file://"
        String fsURI = "file://" + sourcePath;
        log.info("create source for tid: {}, fsURI: {}", tid, fsURI);
        TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path(fsURI));
        format.setFilesFilter(new CsvFilePathFilter());

        long INTERVAL = 100;
        // String for one line, or List<String> for one line?
        // or byte for one line?
        // set uid for source, so we can recover?
        DataStream<String> source = env.readFile(format, fsURI, FileProcessingMode.PROCESS_CONTINUOUSLY, INTERVAL)
                .setParallelism(1).name("source" + tid);

        // file suffix
        // parquet is better? parquet should use bulk format
        // TODO a test way, link to hdfs later
        // if multi sink to one table path, better to use tid-pid as suffix(only one
        // task running for one tid-pid), so redesign the bucket assigner?
        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(sinkPath),
                        (Encoder<String>) (element, stream) -> {
                            PrintStream out = new PrintStream(stream);
                            // TODO(hw): escape?
                            out.println(element);
                        })
                // all to 0/, don't split by hour
                .withBucketAssigner(new KeyBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        // Streaming streamingFileSink
        source.sinkTo(sink);

        JobClient client;
        try {
            client = env.executeAsync(genJobName(tid));
        } catch (Exception e) {
            return false;
        }
        sourceMap.put(tid, client.getJobID());
        log.info("create job for tid: {} success, job {}", tid, client.getJobID());
        return true;
    }

    public JobStatus getJobStatus(int tid) {
        JobID id = sourceMap.get(tid);
        if (id == null) {
            log.error("tid: {} not exist", tid);
            return null;
        }
        try {
            // rpc may failed
            return miniCluster.getJobStatus(id).get();
        } catch (InterruptedException | ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public void close() {

    }

    public String genJobName(int tid) {
        return "stream-tid-" + tid;
    }

    public synchronized void closeTunnel(int tid) {
        JobID id = Preconditions.checkNotNull(sourceMap.get(tid));
        // TODO too soon, the job may not be finished
        // test get job status by rest api
        try {
            URL url = miniClusterAddr.resolve("/jobs/" + id).toURL();
            log.info("check job: {}", url);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            log.info(con.getResponseMessage());
        } catch (Exception e) {
        }
        // targetDirectory: null if the default savepointdirectory should be used
        // terminate, not suspend
        // miniCluster.stopWithSavepoint(id, null, true); // can't?
        miniCluster.cancelJob(id); // checkpoint is retain on cancellation, but it's better to wait a while?

        // task record count? == source count or wahtever?
    }

    /** Use first field for buckets. */
    public static final class KeyBucketAssigner
            implements BucketAssigner<String, String> {
        private static final long serialVersionUID = 987325769970523326L;

        @Override
        public String getBucketId(final String element, final Context context) {
            return "0";
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
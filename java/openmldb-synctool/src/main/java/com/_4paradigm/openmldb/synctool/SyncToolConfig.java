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

import java.io.IOException;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import lombok.*;
import com.google.common.base.Preconditions;

/**
 * The global configuration of SyncTool.
 *
 * Need to call SyncToolConfig.parser() before accessing configuration.
 */
@Slf4j
public class SyncToolConfig {
    public static String HOST;
    public static int PORT;
    public static int WORKER_THREAD;
    public static int IO_THREAD;
    // public static int CHANNEL_KEEP_ALIVE_TIME;
    public static String ZK_CLUSTER;
    public static String ZK_ROOT_PATH;
    public static String ZK_CERT;
    public static String SYNC_TASK_PROGRESS_PATH;

    public static String HADOOP_CONF_DIR;

    public static String DATA_CACHE_PATH;
    public static int FLINK_SLOTS;

    public static int TASK_CHECK_PERIOD; // seconds
    // sync tool->data collector add sync task timeout
    public static int REQUEST_TIMEOUT; // seconds

    private static boolean isParsed = false;
    @Getter
    private static Properties prop = new Properties();

    public synchronized static void parse() {
        if (!isParsed) {
            doParse();
            isParsed = true;
        }
    }

    private static void doParse() {
        try {
            prop.load(SyncToolConfig.class.getClassLoader().getResourceAsStream("synctool.properties"));
        } catch (IOException e) {
            throw new RuntimeException("Fail to load synctool.properties", e);
        }
        parseFromProperties(prop);
    }

    private static void parseFromProperties(Properties prop) {
        HOST = prop.getProperty("server.host", "");
        PORT = Integer.parseInt(prop.getProperty("server.port", ""));
        if (PORT < 1 || PORT > 65535) {
            throw new RuntimeException("server.port invalid port, should be in range of 1 through 65535");
        }
        WORKER_THREAD = Integer.parseInt(prop.getProperty("server.worker_threads", "16"));
        IO_THREAD = Integer.parseInt(prop.getProperty("server.io_threads", "4"));

        ZK_CLUSTER = prop.getProperty("zookeeper.cluster", "");
        if (ZK_CLUSTER.isEmpty()) {
            throw new RuntimeException("zookeeper.cluster should not be empty");
        }

        ZK_ROOT_PATH = prop.getProperty("zookeeper.root_path", "");
        if (ZK_ROOT_PATH.isEmpty()) {
            throw new RuntimeException("zookeeper.root_path should not be empty");
        }
        ZK_CERT = prop.getProperty("zookeeper.cert", "");

        HADOOP_CONF_DIR = prop.getProperty("hadoop.conf.dir", "");
        if (HADOOP_CONF_DIR.isEmpty()) {
            log.info("no hadoop.conf.dir, use env");
            HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
            Preconditions.checkArgument(!HADOOP_CONF_DIR.isEmpty(),
                    "hadoop.conf.dir should not be empty, set in system env or synctool.properties");
        }

        // TODO(hw): just for flink tunnel, not used now
        DATA_CACHE_PATH = prop.getProperty("data.cache_path", "");
        // Preconditions.checkArgument(!DATA_CACHE_PATH.isEmpty(), "data.cache_path
        // should not be empty");

        SYNC_TASK_PROGRESS_PATH = prop.getProperty("sync_task.progress_path", "");
        Preconditions.checkArgument(
                !SYNC_TASK_PROGRESS_PATH.isEmpty(), "sync_task.progress_path should not be empty");

        FLINK_SLOTS = Integer.parseInt(prop.getProperty("flink.slots", "32"));

        TASK_CHECK_PERIOD = Integer.parseInt(prop.getProperty("sync_task.check_period", "10"));

        REQUEST_TIMEOUT = Integer.parseInt(prop.getProperty("request.timeout", "5"));

        log.info("SyncToolConfig: {}", prop);
    }
}

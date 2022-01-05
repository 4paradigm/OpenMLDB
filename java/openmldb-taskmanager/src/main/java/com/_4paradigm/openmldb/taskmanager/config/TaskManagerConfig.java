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

package com._4paradigm.openmldb.taskmanager.config;

import java.util.Properties;

public class TaskManagerConfig {
    public static String HOST = "127.0.0.1";
    public static int PORT = 9902;
    public static int WORKER_THREAD = 4;
    public static int IO_THREAD = 4;
    public static String ZK_CLUSTER;
    public static String ZK_ROOT_PATH;
    public static String ZK_TASKMANAGER_PATH;
    public static String ZK_MAX_JOB_ID_PATH;
    public static int ZK_SESSION_TIMEOUT;
    public static int ZK_CONNECTION_TIMEOUT;
    public static int ZK_BASE_SLEEP_TIME;
    public static int ZK_MAX_CONNECT_WAIT_TIME;
    public static int ZK_MAX_RETRIES;
    public static String OFFLINE_DATA_PREFIX;
    public static String SPARK_MASTER;
    public static String BATCHJOB_JAR_PATH;
    public static String SPARK_YARN_JARS;
    public static String SPARK_HOME;
    public static int PREFETCH_JOBID_NUM;
    public static String NAMENODE_URI;

    static {
        try {
            Properties prop = new Properties();
            prop.load(TaskManagerConfig.class.getClassLoader().getResourceAsStream("taskmanager.properties"));
            HOST = prop.getProperty("server.host", "127.0.0.1");
            PORT = Integer.parseInt(prop.getProperty("server.port", "9902"));
            WORKER_THREAD = Integer.parseInt(prop.getProperty("server.worker_threads", "4"));
            IO_THREAD = Integer.parseInt(prop.getProperty("server.io_threads", "4"));
            ZK_SESSION_TIMEOUT = Integer.parseInt(prop.getProperty("zookeeper.session_timeout", "5000"));
            ZK_CLUSTER = prop.getProperty("zookeeper.cluster", "");
            ZK_ROOT_PATH = prop.getProperty("zookeeper.root_path", "");
            ZK_TASKMANAGER_PATH = ZK_ROOT_PATH + "/taskmanager";
            ZK_MAX_JOB_ID_PATH = ZK_TASKMANAGER_PATH + "/max_job_id";
            ZK_CONNECTION_TIMEOUT = Integer.parseInt(prop.getProperty("zookeeper.connection_timeout", "5000"));
            ZK_BASE_SLEEP_TIME = Integer.parseInt(prop.getProperty("zookeeper.base_sleep_time", "1000"));
            ZK_MAX_RETRIES = Integer.parseInt(prop.getProperty("zookeeper.max_retries", "10"));
            ZK_MAX_CONNECT_WAIT_TIME = Integer.parseInt(prop.getProperty("zookeeper.max_connect_waitTime", "30000"));
            OFFLINE_DATA_PREFIX = prop.getProperty("offline.data.prefix");
            SPARK_MASTER = prop.getProperty("spark.master", "yarn");
            BATCHJOB_JAR_PATH = prop.getProperty("batchjob.jar.path");
            SPARK_YARN_JARS = prop.getProperty("spark.yarn.jars");
            SPARK_HOME = prop.getProperty("spark.home");
            PREFETCH_JOBID_NUM = Integer.parseInt(prop.getProperty("prefetch.jobid.num", "10"));
            NAMENODE_URI= prop.getProperty("namenode.uri", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

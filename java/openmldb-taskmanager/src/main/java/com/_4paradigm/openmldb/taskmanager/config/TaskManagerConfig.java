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

import com._4paradigm.openmldb.taskmanager.util.BatchJobUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * The global configuration of TaskManager.
 *
 * Need to call TaskManagerConfig.parser() before accessing configuration.
 */
public class TaskManagerConfig {
    private static Logger logger = LoggerFactory.getLogger(TaskManagerConfig.class);

    private volatile static TaskManagerConfig instance;

    private volatile static Properties props;

    public static Properties getProps() {
        return props;
    }

    private static TaskManagerConfig getInstance() throws ConfigException {
        if (instance == null) {
            instance = new TaskManagerConfig();
            instance.init();
        }
        return instance;
    }

    public static void parse() throws ConfigException {
        getInstance();
    }

    protected static String getString(String key) {
        return props.getProperty(key);
    }

    protected static int getInt(String key) {
        return Integer.parseInt(getString(key));
    }

    protected static long getLong(String key) {
        return Long.parseLong(getString(key));
    }

    protected static boolean getBool(String key) {
        return Boolean.parseBoolean(getString(key));
    }


    public static String getServerHost() {
        return getString("server.host");
    }

    public static int getServerPort() {
        return getInt("server.port");
    }

    public static int getServerWorkerThreads() {
        return getInt("server.worker_threads");
    }

    public static int getServerIoThreads() {
        return getInt("server.io_threads");
    }

    public static int getChannelKeepAliveTime() {
        return getInt("server.channel_keep_alive_time");
    }

    public static int getZkSessionTimeout() {
        return getInt("zookeeper.session_timeout");
    }

    public static String getZkCluster() {
        return getString("zookeeper.cluster");
    }

    public static String getZkRootPath() {
        return getString("zookeeper.root_path");
    }

    public static int getZkConnectionTimeout() {
        return getInt("zookeeper.connection_timeout");
    }

    public static int getZkBaseSleepTime() {
        return getInt("zookeeper.base_sleep_time");
    }

    public static int getZkMaxRetries() {
        return getInt("zookeeper.max_retries");
    }

    public static int getZkMaxConnectWaitTime() {
        return getInt("zookeeper.max_connect_waitTime");
    }

    public static String getSparkMaster() {
        return getString("spark.master");
    }

    public static String getSparkYarnJars() {
        return getString("spark.yarn.jars");
    }

    public static String getSparkHome() {
        return getString("spark.home");
    }

    public static int getPrefetchJobidNum() {
        return getInt("prefetch.jobid.num");
    }

    public static String getJobLogPath() {
        return getString("job.log.path");
    }

    public static String getExternalFunctionDir() {
        return getString("external.function.dir");
    }

    public static boolean getTrackUnfinishedJobs() {
        return getBool("track.unfinished.jobs");
    }

    public static int getJobTrackerInterval() {
        return getInt("job.tracker.interval");
    }

    public static String getSparkDefaultConf() {
        return getString("spark.default.conf");
    }

    public static String getSparkEventlogDir() {
        return getString("spark.eventLog.dir");
    }

    public static int getSparkYarnMaxappattempts() {
        return getInt("spark.yarn.maxAppAttempts");
    }

    public static String getOfflineDataPrefix() {
        return getString("offline.data.prefix");
    }

    public static String getBatchjobJarPath() {
        return getString("batchjob.jar.path");
    }


    public static String getHadoopConfDir() {
        return getString("hadoop.conf.dir");
    }

    public static boolean getEnableHiveSupport() {
        return getBool("enable.hive.support");
    }

    public static long getBatchJobResultMaxWaitTime() {
        return getLong("batch.job.result.max.wait.time");
    }


    public static String getK8sHadoopConfigmapName() {
        return getString("k8s.hadoop.configmap");
    }

    public static String getK8sMountLocalPath() {
        return getString("k8s.mount.local.path");
    }

    public static String getHadoopUserName() {
        return getString("hadoop.user.name");
    }

    public static String getZkTaskmanagerPath() {
        return getZkRootPath() + "/taskmanager";
    }

    public static String getZkMaxJobIdPath() {
        return getZkTaskmanagerPath() + "/max_job_id";
    }

    public static boolean isK8s() {
        return getSparkMaster().equals("k8s") || getSparkMaster().equals("kubernetes");
    }

    public static boolean isYarnCluster() {
        return getSparkMaster().equals("yarn") || getSparkMaster().equals("yarn-cluster");
    }

    public static boolean isYarn() {
        return getSparkMaster().startsWith("yarn");
    }

    public static void print() throws ConfigException {
        parse();

        StringBuilder builder = new StringBuilder();

        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            builder.append(key + " = " + value + "\n");
        }

        logger.info("Final TaskManager config: \n" + builder.toString());
    }

    private void init() throws ConfigException {
        props = new Properties();

        // Load local properties file
        try {
            props.load(TaskManagerConfig.class.getClassLoader().getResourceAsStream("taskmanager.properties"));
        } catch (IOException e) {
            throw new ConfigException(String.format("Fail to load taskmanager.properties, message: ", e.getMessage()));
        }

        // Get properties and check
        if (props.getProperty("server.host") == null) {
            props.setProperty("server.host", "0.0.0.0");
        }

        if (props.getProperty("server.port") == null) {
            props.setProperty("server.port", "9902");
        }

        if (getServerPort() < 1 || getServerPort() > 65535) {
            throw new ConfigException("server.port", "invalid port, should be in range of 1 through 65535");
        }

        if (props.getProperty("server.worker_threads") == null) {
            props.setProperty("server.worker_threads", "16");
        }

        if (getServerWorkerThreads() <= 0) {
            throw new ConfigException("server.worker_threads", "should be larger than 0");
        }

        if (props.getProperty("server.io_threads") == null) {
            props.setProperty("server.io_threads", "4");
        }

        if (getServerIoThreads() <= 0) {
            throw new ConfigException("server.io_threads", "should be larger than 0");
        }

        if (props.getProperty("server.channel_keep_alive_time") == null) {
            props.setProperty("server.channel_keep_alive_time", "1800");
        }

        if (getChannelKeepAliveTime() <= 0) {
            throw new ConfigException("server.channel_keep_alive_time", "should be larger than 0");
        }

        if (props.getProperty("zookeeper.session_timeout") == null) {
            props.setProperty("zookeeper.session_timeout", "5000");
        }

        if (getZkSessionTimeout() <= 0) {
            throw new ConfigException("zookeeper.session_timeout", "should be larger than 0");
        }

        if (props.getProperty("zookeeper.cluster") == null) {
            props.setProperty("", "");
        }

        if (isEmpty(getZkCluster())) {
            throw new ConfigException("zookeeper.cluster", "should not be empty");
        }

        if (props.getProperty("zookeeper.connection_timeout") == null) {
            props.setProperty("zookeeper.connection_timeout", "5000");
        }

        if (getZkConnectionTimeout() <= 0) {
            throw new ConfigException("zookeeper.connection_timeout", "should be larger than 0");
        }

        if (props.getProperty("zookeeper.base_sleep_time") == null) {
            props.setProperty("zookeeper.base_sleep_time", "1000");
        }

        if (getZkBaseSleepTime() <= 0) {
            throw new ConfigException("zookeeper.base_sleep_time", "should be larger than 0");
        }

        if (props.getProperty("zookeeper.max_retries") == null) {
            props.setProperty("zookeeper.max_retries", "10");
        }

        if (getZkMaxRetries() <= 0) {
            throw new ConfigException("zookeeper.max_retries", "should be larger than 0");
        }

        if (props.getProperty("zookeeper.max_connect_waitTime") == null) {
            props.setProperty("zookeeper.max_connect_waitTime", "30000");
        }

        if (getZkMaxConnectWaitTime() <= 0) {
            throw new ConfigException("zookeeper.max_connect_waitTime", "should be larger than 0");
        }

        if (props.getProperty("spark.master") == null) {
            props.setProperty("spark.master", "local[*]");
        } else {
            props.setProperty("spark.master", props.getProperty("spark.master").toLowerCase());
        }

        if (!getSparkMaster().startsWith("local")) {
            if (!Arrays.asList("yarn", "yarn-cluster", "yarn-client", "k8s", "kubernetes").contains(getSparkMaster())) {
                throw new ConfigException("spark.master", "should be local, yarn, yarn-cluster, yarn-client, k8s or kubernetes");
            }
        }

        if (props.getProperty("spark.yarn.jars") == null) {
            props.setProperty("spark.yarn.jars", "");
        }

        if (isYarn() && !isEmpty(getSparkYarnJars()) && getSparkYarnJars().startsWith("file://")) {
            throw new ConfigException("spark.yarn.jars", "should not use local filesystem for yarn mode");
        }


        if (isEmpty(props.getProperty("spark.home", ""))) {
            if (System.getenv("SPARK_HOME") == null) {
                throw new ConfigException("spark.home", "should set config 'spark.home' or environment variable 'SPARK_HOME'");
            } else {
                logger.info("Use SPARK_HOME from environment variable: " + System.getenv("SPARK_HOME"));
                props.setProperty("spark.home", System.getenv("SPARK_HOME"));
            }
        }

        String SPARK_HOME = firstNonEmpty(props.getProperty("spark.home"), System.getenv("SPARK_HOME"));
        // isEmpty checks null and empty
        if (isEmpty(SPARK_HOME)) {
            throw new ConfigException("spark.home", "should set config 'spark.home' or environment variable 'SPARK_HOME'");
        }
        if (SPARK_HOME != null) {
            props.setProperty("spark.home", SPARK_HOME);
        }

        // TODO: Check if we can get spark-submit

        if (props.getProperty("prefetch.jobid.num") == null) {
            props.setProperty("prefetch.jobid.num", "1");
        }

        if (getPrefetchJobidNum() < 1) {
            throw new ConfigException("prefetch.jobid.num", "should be larger or equal to 1");
        }

        if (props.getProperty("job.log.path") == null) {
            props.setProperty("job.log.path", "../log/");
        }

        if (getJobLogPath().startsWith("hdfs") || getJobLogPath().startsWith("s3")) {
            throw new ConfigException("job.log.path", "only support local filesystem");
        }

        File jobLogDirectory = new File(getJobLogPath());
        if (!jobLogDirectory.exists()) {
            logger.info("The log path does not exist, try to create directory: " + getJobLogPath());
            jobLogDirectory.mkdirs();
            if (!jobLogDirectory.exists()) {
                throw new ConfigException("job.log.path", "fail to create log path: " + jobLogDirectory);
            }
        }

        if (props.getProperty("external.function.dir") == null) {
            props.setProperty("external.function.dir", "./udf/");
        }

        File externalFunctionDir = new File(getExternalFunctionDir());
        if (!externalFunctionDir.exists()) {
            logger.info("The external function dir does not exist, try to create directory: "
                    + getExternalFunctionDir());
            externalFunctionDir.mkdirs();
            if (!externalFunctionDir.exists()) {
                throw new ConfigException("job.log.path", "fail to create external function path: " + externalFunctionDir);
            }
        }

        if (props.getProperty("track.unfinished.jobs") == null) {
            props.setProperty("track.unfinished.jobs", "true");
        }

        if (props.getProperty("job.tracker.interval") == null) {
            props.setProperty("job.tracker.interval", "30");
        }

        if (getJobTrackerInterval() <= 0) {
            throw new ConfigException("job.tracker.interval", "should be larger than 0");
        }

        if (props.getProperty("spark.default.conf") == null) {
            props.setProperty("spark.default.conf", "");
        }

        if (!isEmpty(getSparkDefaultConf())) {
            String[] defaultSparkConfs = getSparkDefaultConf().split(";");
            for (String sparkConfMap : defaultSparkConfs) {
                if (!isEmpty(sparkConfMap)) {
                    String[] kv = sparkConfMap.split("=");
                    if (kv.length < 2) {
                        throw new ConfigException("spark.default.conf", String.format("error format of %s", sparkConfMap));
                    } else if (!kv[0].startsWith("spark")) {
                        throw new ConfigException("spark.default.conf", String.format("config key should start with 'spark' but get %s", kv[0]));
                    }
                }
            }
        }

        if (props.getProperty("spark.eventLog.dir") == null) {
            props.setProperty("spark.eventLog.dir", "");
        }

        if (!isEmpty(getSparkEventlogDir()) && isYarn()) {
            if (getSparkEventlogDir().startsWith("file://")) {
                throw new ConfigException("spark.eventLog.dir", "should not use local filesystem for yarn mode");
            }
        }

        if (props.getProperty("spark.yarn.maxAppAttempts") == null) {
            props.setProperty("spark.yarn.maxAppAttempts", "1");
        }

        if (getSparkYarnMaxappattempts() <= 0) {
            throw new ConfigException("spark.yarn.maxAppAttempts", "should be larger than 0");
        }


        if (props.getProperty("offline.data.prefix") == null) {
            props.setProperty("offline.data.prefix", "file:///tmp/openmldb_offline_storage/");
        }

        if (isEmpty(getOfflineDataPrefix())) {
            throw new ConfigException("offline.data.prefix", "should not be null");
        } else {
            if (isYarn() || isK8s()) {
                if (getOfflineDataPrefix().startsWith("file://")) {
                    throw new ConfigException("offline.data.prefix", "should not use local filesystem for yarn mode or k8s mode");
                }
            }
        }

        if (isEmpty(props.getProperty("batchjob.jar.path", ""))) {
            props.setProperty("batchjob.jar.path", BatchJobUtil.findLocalBatchJobJar());
        }

        if (isYarn() && isEmpty(getHadoopConfDir())) {
            if (System.getenv("HADOOP_CONF_DIR") == null) {
                throw new ConfigException("hadoop.conf.dir", "should set config 'hadoop.conf.dir' or environment variable 'HADOOP_CONF_DIR'");
            } else {
                // TODO: Check if we can get core-site.xml
                props.setProperty("hadoop.conf.dir", System.getenv("HADOOP_CONF_DIR"));
            }
        }

        // TODO(hw): need default root?
        String HADOOP_USER_NAME = firstNonEmpty(props.getProperty("hadoop.user.name"),  System.getenv("HADOOP_USER_NAME"));
        if (HADOOP_USER_NAME != null) {
            props.setProperty("hadoop.user.name", HADOOP_USER_NAME);
        }


        String HADOOP_CONF_DIR = firstNonEmpty(props.getProperty("hadoop.conf.dir"), System.getenv("HADOOP_CONF_DIR"));
        if (isYarn() && isEmpty(HADOOP_CONF_DIR)) {
            throw new ConfigException("hadoop.conf.dir", "should set config 'hadoop.conf.dir' or environment variable 'HADOOP_CONF_DIR'");
        }
        if (HADOOP_CONF_DIR != null) {
            props.setProperty("hadoop.conf.dir", HADOOP_CONF_DIR);
        }


        if (props.getProperty("enable.hive.support") == null) {
            props.setProperty("enable.hive.support", "true");
        }

        if (props.getProperty("batch.job.result.max.wait.time") == null) {
            props.setProperty("batch.job.result.max.wait.time", "600000");
        }

        if (props.getProperty("k8s.hadoop.configmap") == null) {
            props.setProperty("k8s.hadoop.configmap", "hadoop-config");
        }

        if (props.getProperty("k8s.mount.local.path") == null) {
            props.setProperty("k8s.mount.local.path", "/tmp");
        }
    }


    // ref org.apache.spark.launcher.CommandBuilderUtils
    public static String firstNonEmpty(String... strings) {
        for (String s : strings) {
            if (!isEmpty(s)) {
                return s;
            }
        }
        return null;
    }


    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

}


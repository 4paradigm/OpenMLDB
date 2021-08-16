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

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FileSystemManagerTest {
    private static final Logger logger = Logger.getLogger(FileSystemManager.class);
    private String testHdfsHost = "hdfs://host:port";
    private MiniDFSCluster hdfsCluster;
    private FileSystemManager fileSystemManager;

    @Before
    public void setUp() throws Exception {
        Logger.getRootLogger().setLevel(Level.WARN);
        File baseDir = new File("./target/hdfs/minicluster").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        Configuration conf = new Configuration();
        conf.setQuietMode(true);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        // DFSConfigKeys can't work?
        // conf.set(DFS_REPLICATION_KEY, "1");
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        builder.skipFsyncForTesting(true);
        // will cost ～50 seconds
        hdfsCluster = builder.format(true).build();
        Logger.getRootLogger().setLevel(Level.INFO);
        testHdfsHost = "hdfs://localhost:" + hdfsCluster.getNameNodePort();
        logger.info("dfs is on: " + testHdfsHost);

        // TODO
        fileSystemManager = new FileSystemManager();

    }

    @Test
    public void testGetFileSystemSuccess() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(testHdfsHost), conf);
        // create the directory using the default permission
        Path createPath = new Path(testHdfsHost, "/test_dir");
        boolean result = fs.mkdirs(createPath);
        Assert.assertTrue(result);

        logger.info(fs.listStatus(createPath));

        // TODO support orc, parquet later
        Path csvPath = new Path(createPath, "/data1.csv");
        FSDataOutputStream fsOut = fs.create(csvPath);
        // write
        CsvWriter csvWriter = new CsvWriter(fsOut, ',', StandardCharsets.UTF_8);
        // 写表头
        String[] csvHeaders = {"int_16", "int_32", "int_64", "string", "float_1", "double_1", "boolean_1", "date", "timestamp"};
        csvWriter.writeRecord(csvHeaders);
        // 写内容
        String[] arr = {"1", "22", "33", "ss", "5", "6", "true", "1994-10-08", "1994-10-08 00:00:00"};
        for (int i = 0; i < 2; i++) {
            String[] csvContent = {arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7], arr[8]};
            csvWriter.writeRecord(csvContent);
        }
        csvWriter.close();

        fsOut.close();

        logger.info(fs.getFileStatus(csvPath));

        // read csv from hdfs
        FSDataInputStream inStream = fs.open(csvPath);
        // 用来保存数据
        ArrayList<String[]> csvFileList = new ArrayList<>();
        // 创建CSV读对象 例如:CsvReader(文件路径，分隔符，编码格式);
        CsvReader reader = new CsvReader(inStream, ',', StandardCharsets.UTF_8);
        // 跳过表头 如果需要表头的话，这句可以忽略
        reader.readHeaders(); // TODO(hw): check schema if exists

        // TODO(hw): InsertPreparedStatementImpl

    }

//    @Test
//    public void testGetFileSystemForhHA() throws IOException {
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("username", "user");
//        properties.put("password", "passwd");
//        properties.put("fs.defaultFS", "hdfs://palo");
//        properties.put("dfs.nameservices", "palo");
//        properties.put("dfs.ha.namenodes.palo", "nn1,nn2");
//        properties.put("dfs.namenode.rpc-address.palo.nn1", "host1:port1");
//        properties.put("dfs.namenode.rpc-address.palo.nn2", "host2:port2");
//        properties.put("dfs.client.failover.proxy.provider.bdos",
//                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//        BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
//        assertNotNull(fs);
//        fs.getDFSFileSystem().close();
//    }
//
//    @Test
//    public void testGetFileSystemForHAWithNoNames() throws IOException {
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("username", "user");
//        properties.put("password", "passwd");
//        properties.put("fs.defaultFS", "hdfs://palo");
//        properties.put("dfs.nameservices", "palo");
//        properties.put("dfs.client.failover.proxy.provider.bdos",
//                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//        boolean haveException = false;
//        try {
//            BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
//        } catch (BrokerException be) {
//            haveException = true;
//        }
//        assertEquals(true, haveException);
//    }
//
//    @Test
//    public void testGetFileSystemForHAWithNoRpcConfig() throws IOException {
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("username", "user");
//        properties.put("password", "passwd");
//        properties.put("fs.defaultFS", "hdfs://palo");
//        properties.put("dfs.nameservices", "palo");
//        properties.put("dfs.ha.namenodes.palo", "nn1,nn2");
//        properties.put("dfs.namenode.rpc-address.palo.nn1", "host1:port1");
//        properties.put("dfs.client.failover.proxy.provider.bdos",
//                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
//        boolean haveException = false;
//        try {
//            BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
//        } catch (BrokerException be) {
//            haveException = true;
//        }
//        assertEquals(true, haveException);
//    }
//
//    @Test
//    public void testGetFileSystemForHAWithNoProviderArguments() throws IOException {
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("username", "user");
//        properties.put("password", "passwd");
//        properties.put("fs.defaultFS", "hdfs://palo");
//        properties.put("dfs.nameservices", "palo");
//        properties.put("dfs.ha.namenodes.palo", "nn1,nn2");
//        properties.put("dfs.namenode.rpc-address.palo.nn1", "host1:port1");
//        properties.put("dfs.namenode.rpc-address.palo.nn2", "host2:port2");
//        BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
//        assertNotNull(fs);
//        fs.getDFSFileSystem().close();
//    }
//
//    @Test
//    public void testGetFileSystemWithoutPassword() throws IOException {
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("username", "user");
//        // properties.put("password", "changeit");
//        boolean haveException = false;
//        try {
//            BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
//        } catch (BrokerException e) {
//            haveException = true;
//        }
//        assertEquals(true, haveException);
//    }
//
//    @Test
//    public void testListPaths() {
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("username", "user");
//        properties.put("password", "passwd");
//
//        List<TBrokerFileStatus> files2 = fileSystemManager.listPath(testHdfsHost + "/data/abc/logs/*.out",
//                false, properties);
//        assertEquals(files2.size(), 2);
//    }
//
//    @Test
//    public void testOpenFileStream() {
//        String realClientId = "realClientId";
//        String fokeClientId = "fokeClientId";
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("username", "root");
//        properties.put("password", "passwd");
//
//        String tempFile = testHdfsHost + "/data/abc/logs/" + System.nanoTime() + ".txt";
//        boolean isPathExist = fileSystemManager.checkPathExist(tempFile, properties);
//        assertFalse(isPathExist);
//
//        // test openwriter
//        TBrokerFD writeFd = fileSystemManager.openWriter(realClientId, tempFile, properties);
//        // test write
//        byte[] dataBuf = new byte[1256];
//        fileSystemManager.pwrite(writeFd, 0, dataBuf);
//        // close writer
//        fileSystemManager.closeWriter(writeFd);
//        isPathExist = fileSystemManager.checkPathExist(tempFile, properties);
//        assertTrue(isPathExist);
//
//        // check file size
//        List<TBrokerFileStatus> files = fileSystemManager.listPath(tempFile, false, properties);
//        assertEquals(files.size(), 1);
//        assertFalse(files.get(0).isDir);
//        assertEquals(1256, files.get(0).size);
//
//        // rename file
//        String tempFile2 = testHdfsHost + "/data/abc/logs/" + System.nanoTime() + ".txt";
//        fileSystemManager.renamePath(tempFile, tempFile2, properties);
//        isPathExist = fileSystemManager.checkPathExist(tempFile, properties);
//        assertFalse(isPathExist);
//        isPathExist = fileSystemManager.checkPathExist(tempFile2, properties);
//        assertTrue(isPathExist);
//
//        // read file
//        TBrokerFD readFd = fileSystemManager.openReader(realClientId, tempFile2, 0, properties);
//        ByteBuffer readData = fileSystemManager.pread(readFd, 0, 2222);
//        assertEquals(1256, readData.limit());
//
//        // read with exception
//        boolean readDataHasError = false;
//        try {
//            ByteBuffer readData2 = fileSystemManager.pread(readFd, 1, 2222);
//        } catch (BrokerException e) {
//            readDataHasError = true;
//            assertEquals(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET, e.errorCode);
//        }
//        assertEquals(true, readDataHasError);
//
//        // delete file
//        fileSystemManager.deletePath(tempFile2, properties);
//        isPathExist = fileSystemManager.checkPathExist(tempFile2, properties);
//        assertFalse(isPathExist);
//    }
//
//    @Test
//    public void testGetFileSystemForS3aScheme() throws IOException {
//        Map<String, String> properties = new HashMap<String, String>();
//        properties.put("fs.s3a.access.key", "accessKey");
//        properties.put("fs.s3a.secret.key", "secretKey");
//        properties.put("fs.s3a.endpoint", "s3.test.com");
//        BrokerFileSystem fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties);
//        assertNotNull(fs);
//        fs.getDFSFileSystem().close();
//    }
}

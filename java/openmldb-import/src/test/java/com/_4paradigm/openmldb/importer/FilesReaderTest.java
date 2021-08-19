/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.importer;

import junit.framework.TestCase;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class FilesReaderTest extends TestCase {
    private static final Logger logger = Logger.getLogger(FilesReaderTest.class);
    private MiniDFSCluster hdfsCluster;
    private Path hdfsFilePath;

    @Override
    protected void setUp() throws Exception {
        // ignore hdfs info log
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
        String testHdfsHost = "hdfs://localhost:" + hdfsCluster.getNameNodePort();
        logger.info("created hdfs is on: " + testHdfsHost);

        logger.info("create a file on hdfs");
        FileSystem fs = FileSystem.get(URI.create(testHdfsHost), conf);
        // create the directory using the default permission
        Path createPath = new Path(testHdfsHost, "/test_dir");
        boolean result = fs.mkdirs(createPath);
        Assert.assertTrue(result);

        hdfsFilePath = new Path(createPath, "/data1.csv");
        FSDataOutputStream fsOut = fs.create(hdfsFilePath);

        // write some data
        CSVFormat format = CSVFormat.Builder.create().setHeader().build();
        CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(fsOut, StandardCharsets.UTF_8), format);
        // printHeaders needs ResultSet, use printRecord to print headers for simplicity
        List<String> csvHeaders = Arrays.asList("int_16", "int_32", "int_64", "string", "float_1", "double_1", "boolean_1", "date", "timestamp");
        printer.printRecord(csvHeaders);

        List<String> arr = Arrays.asList("1", "22", "33", "ss", "5", "6", "true", "1994-10-08", "1994-10-08 00:00:00");
        for (int i = 0; i < 2; i++) {
            printer.printRecord(arr);
        }
        printer.close(true);
        fsOut.close();

        logger.info("hdfs file status: " + fs.getFileStatus(hdfsFilePath));
    }

    @Override
    protected void tearDown() {
        hdfsCluster.shutdown(true);
    }

    public void testMixedFiles() {
        FilesReader filesReader = new FilesReader(Arrays.asList("src/test/resources/train.csv.small", hdfsFilePath.toString()));
        try {
            CSVRecord record;
            int recordCount = 0;
            while ((record = filesReader.next()) != null) {
                logger.info(record);
                recordCount++;
            }
            // small has 9 lines, hdfs has 2
            Assert.assertEquals(9 + 2, recordCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
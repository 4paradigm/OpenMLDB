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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com._4paradigm.openmldb.proto.Common.ColumnDesc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

class HDFSTunnel {

    private static HDFSTunnel instance;

    public static synchronized HDFSTunnel getInstance() {
        if (instance == null) {
            instance = new HDFSTunnel();
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

    private FileSystem fileSystem;
    // <tid, sinkPath>
    private Map<Integer, String> sourceMap = new ConcurrentHashMap<>();

    private void init(Properties prop) throws Exception {
        String confDir = SyncToolConfig.HADOOP_CONF_DIR;
        Configuration configuration = new Configuration();
        configuration.addResource(new Path(confDir + "/core-site.xml"));
        configuration.addResource(new Path(confDir + "/hdfs-site.xml"));
        // else will throw org.apache.hadoop.fs.UnsupportedFileSystemException: No
        // FileSystem for scheme "hdfs"
        if (configuration.get("fs.hdfs.impl") == null) {
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        }
        fileSystem = FileSystem.get(configuration);
    }

    public boolean recoverTunnel(int tid, String sinkPath) {
        return createTunnel(tid, null, sinkPath);
    }

    // for hdfs, sourcePath is useless, write to hdfs sinkPath
    public synchronized boolean createTunnel(int tid, String sourcePath, String sinkPath) {
        Preconditions.checkState(!sourceMap.containsKey(tid), "tunnel already exists for tid " + tid);
        // create the directory
        Path path = new Path(sinkPath);
        try {
            if (!fileSystem.exists(path)) {
                fileSystem.mkdirs(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        // end with /
        if (!sinkPath.endsWith("/")) {
            sinkPath += "/";
        }
        sourceMap.put(tid, sinkPath);
        return true;
    }

    public void closeTunnel(int tid) {
        sourceMap.remove(tid);
    }

    // not thread safe
    public void writeData(int tid, ByteBuf data, long count, List<ColumnDesc> columnDescList) {
        try {
            DataParser parser = new DataParser(data, count, columnDescList);
            String sinkPath = sourceMap.get(tid);
            String fileName = SyncToolImpl.uniqueFileName();
            Path hdfsWritePath = new Path(sinkPath + fileName);
            // Progressable?
            FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath);
            BufferedWriter bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            parser.writeAll(bufferedWriter);
            bufferedWriter.close();
        } catch (Exception e) {
            throw new RuntimeException("write to hdfs failed", e);
        }
    }

}

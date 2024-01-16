/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.taskmanager.zk;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * ref
 * https://github.com/apache/hbase/blob/25e9228e2c0a9a752db02e48d55010e0197fd203/hbase-zookeeper/src/main/java/org/apache/hadoop/hbase/zookeeper/RecoverableZooKeeper.java
 * It's a thread-safe class. No opentelemetry trace, and no retry mechanism. If
 * we need retry, we can include RetryCounter.
 */
@ThreadSafe
public class RecoverableZooKeeper {
    private static final Logger LOG = LoggerFactory.getLogger(RecoverableZooKeeper.class);
    // the actual ZooKeeper client instance
    private ZooKeeper zk;
    // private final RetryCounterFactory retryCounterFactory;
    // An identifier of this process in the cluster
    private final String identifier;
    private final byte[] id;
    private final Watcher watcher;
    private final int sessionTimeout;
    private final String quorumServers;
    private final int maxMultiSize; // unused now

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DE_MIGHT_IGNORE", justification = "None. Its always been this way.")
    public RecoverableZooKeeper(String quorumServers, int sessionTimeout, Watcher watcher) throws IOException {
        // TODO: Add support for zk 'chroot'; we don't add it to the quorumServers
        // String as we should.
        String identifier = null;
        if (identifier == null || identifier.length() == 0) {
            // the identifier = processID@hostName
            identifier = ManagementFactory.getRuntimeMXBean().getName();
        }
        LOG.info("Process identifier={} connecting to ZooKeeper ensemble={}", identifier, quorumServers);
        this.identifier = identifier;
        this.id = identifier.getBytes(StandardCharsets.UTF_8.name());

        this.watcher = watcher;
        this.sessionTimeout = sessionTimeout;
        this.quorumServers = quorumServers;
        this.maxMultiSize = 1024 * 1024;

        try {
            checkZk();
        } catch (Exception x) {
            /* ignore */
        }
    }

    /**
     * Returns the maximum size (in bytes) that should be included in any single
     * multi() call. NB:
     * This is an approximation, so there may be variance in the msg actually sent
     * over the wire.
     * Please be sure to set this approximately, with respect to your ZK server
     * configuration for
     * jute.maxbuffer.
     */
    public int getMaxMultiSizeLimit() {
        return maxMultiSize;
    }

    /**
     * Try to create a ZooKeeper connection. Turns any exception encountered into a
     * KeeperException.OperationTimeoutException so it can retried.
     * 
     * @return The created ZooKeeper connection object
     * @throws KeeperException if a ZooKeeper operation fails
     */
    protected synchronized ZooKeeper checkZk() throws KeeperException {
        if (this.zk == null) {
            try {
                this.zk = new ZooKeeper(quorumServers, sessionTimeout, watcher);
            } catch (IOException ex) {
                LOG.warn("Unable to create ZooKeeper Connection", ex);
                throw new KeeperException.OperationTimeoutException();
            }
        }
        return zk;
    }

    public synchronized void reconnectAfterExpiration() throws IOException, KeeperException, InterruptedException {
        if (zk != null) {
            LOG.info("Closing dead ZooKeeper connection, session" + " was: 0x" + Long.toHexString(zk.getSessionId()));
            zk.close();
            // reset the ZooKeeper connection
            zk = null;
        }
        checkZk();
        LOG.info("Recreated a ZooKeeper, session" + " is: 0x" + Long.toHexString(zk.getSessionId()));
    }

    public synchronized long getSessionId() {
        return zk == null ? -1 : zk.getSessionId();
    }

    public synchronized void close() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    public synchronized States getState() {
        return zk == null ? null : zk.getState();
    }

    public synchronized ZooKeeper getZooKeeper() {
        return zk;
    }

    public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) throws KeeperException {
        checkZk().sync(path, cb, ctx);
    }

    /**
     * Filters the given node list by the given prefixes. This method is
     * all-inclusive--if any element
     * in the node list starts with any of the given prefixes, then it is included
     * in the result.
     * 
     * @param nodes    the nodes to filter
     * @param prefixes the prefixes to include in the result
     * @return list of every element that starts with one of the prefixes
     */
    private static List<String> filterByPrefix(List<String> nodes, String... prefixes) {
        List<String> lockChildren = new ArrayList<>();
        for (String child : nodes) {
            for (String prefix : prefixes) {
                if (child.startsWith(prefix)) {
                    lockChildren.add(child);
                    break;
                }
            }
        }
        return lockChildren;
    }

    public String getIdentifier() {
        return identifier;
    }
}

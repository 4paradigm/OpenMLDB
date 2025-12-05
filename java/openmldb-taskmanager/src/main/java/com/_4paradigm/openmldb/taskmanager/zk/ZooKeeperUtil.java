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

package com._4paradigm.openmldb.taskmanager.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * Most code is from HBase ZKUtil and ZooKeeperWatcher is replaced with FailoverWatcher.
 */
public class ZooKeeperUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);

    public static boolean watchAndCheckExists(FailoverWatcher failoverWatcher, String znode) throws KeeperException {
        try {
            Stat s = failoverWatcher.getZooKeeper().exists(znode, failoverWatcher);
            boolean exists = s != null;
            if (LOG.isDebugEnabled()) {
                if (exists) {
                    LOG.debug("Set watcher on existing znode " + znode);
                } else {
                    LOG.debug(znode + " does not exist. Watcher is set.");
                }
            }
            return exists;
        } catch (KeeperException e) {
            LOG.warn("Unable to set watcher on znode " + znode, e);
            LOG.warn("Received unexpected KeeperException, re-throwing exception");
            throw e;
        } catch (InterruptedException e) {
            LOG.warn("Unable to set watcher on znode " + znode, e);
            return false;
        }
    }

    public static boolean createEphemeralNodeAndWatch(FailoverWatcher failoverWatcher, String znode, byte[] data)
            throws KeeperException {
        try {
            LOG.info("Try to create emphemeral znode " + znode);
            failoverWatcher.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException nee) {
            if (!watchAndCheckExists(failoverWatcher, znode)) {
                // It did exist but now it doesn't, try again
                return createEphemeralNodeAndWatch(failoverWatcher, znode, data);
            }
            return false;
        } catch (InterruptedException e) {
            LOG.info("Interrupted", e);
            Thread.currentThread().interrupt();
        }
        return true;
    }

    public static void deleteNode(FailoverWatcher failoverWatcher, String node) throws KeeperException {
        deleteNode(failoverWatcher, node, -1);
    }

    public static boolean deleteNode(FailoverWatcher failoverWatcher, String node, int version) throws KeeperException {
        try {
            failoverWatcher.getZooKeeper().delete(node, version);
            return true;
        } catch (KeeperException.BadVersionException bve) {
            LOG.debug("Bad version exception when delete node '{}'", node, bve);
            return false;
        } catch (InterruptedException ie) {
            LOG.debug("Received InterruptedException, doing nothing here", ie);
            return false;
        }
    }

    public static byte[] getDataAndWatch(FailoverWatcher failoverWatcher, String znode) {
        return getDataInternal(failoverWatcher, znode, null);
    }

    @Nullable
    private static byte[] getDataInternal(FailoverWatcher failoverWatcher, String znode, Stat stat) {
        try {
            byte[] data = failoverWatcher.getZooKeeper().getData(znode, failoverWatcher, stat);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Retrieved " + ((data == null) ? 0 : data.length) + " byte(s) of data from znode " + znode);
            }
            return data;
        } catch (KeeperException.NoNodeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unable to get data of znode " + znode + " " + "because node does not exist (not an error)");
            }
            return null;
        } catch (KeeperException | InterruptedException e) {
            LOG.warn("Unable to get data of znode " + znode, e);
            LOG.warn("Received unexpected KeeperException, re-throwing exception");
            return null;
        }
    }

    public static void createAndFailSilent(FailoverWatcher failoverWatcher, String znode)
            throws KeeperException, InterruptedException {
        try {
            LOG.info("Try to create persistent znode " + znode);
            ZooKeeper zk = failoverWatcher.getZooKeeper();
            if (zk.exists(znode, false) == null) {
                zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException ignore) {
            // we just ignore result if the node already exist
            LOG.info("Znode " + znode + " already exist");
        } catch (KeeperException.NoAuthException nee) {
            try {
                if (null == failoverWatcher.getZooKeeper().exists(znode, false)) {
                    // If we failed to create the file and it does not already exist.
                    throw nee;
                }
            } catch (InterruptedException ie) {
                LOG.debug("Received InterruptedException, re-throw the exception", ie);
                throw ie;
            }
        } catch (InterruptedException ie) {
            LOG.debug("Received InterruptedException, re-throw the exception", ie);
            throw ie;
        }
    }

}

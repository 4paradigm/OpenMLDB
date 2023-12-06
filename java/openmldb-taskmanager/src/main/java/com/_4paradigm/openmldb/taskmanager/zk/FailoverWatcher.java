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

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Like ActiveMasterManager in HBase, FailoverWatcher implements master/backup servers switching
 * with ZooKeeper. It will store the information of servers in znode and deal with the detail of
 * blocking and notification for leader election.
 */
public class FailoverWatcher implements Watcher {
    private static final Log LOG = LogFactory.getLog(FailoverWatcher.class);

    private final String baseZnode;
    private final String masterZnode;
    private final String zkQuorum;
    private final int sessionTimeout;
    private final int connectRetryTimes;
    private final HostPort hostPort;
    private RecoverableZooKeeper zooKeeper; // thread-safe
    private final AtomicBoolean connected = new AtomicBoolean(false); // record zookeeper connection status
    private final AtomicBoolean hasActiveServer = new AtomicBoolean(false);
    private final AtomicBoolean becomeActiveServer = new AtomicBoolean(false);

    /**
     * Initialize FailoverWatcher with properties.
     *
     * @throws IOException throw when can't connect with ZooKeeper
     */
    public FailoverWatcher() throws IOException {

        baseZnode = TaskManagerConfig.getZkRootPath() + "/taskmanager";
        masterZnode = baseZnode + "/leader";
        zkQuorum = TaskManagerConfig.getZkCluster();
        sessionTimeout = TaskManagerConfig.getZkSessionTimeout();
        connectRetryTimes = 3;
        String serverHost = TaskManagerConfig.getServerHost();
        int serverPort = TaskManagerConfig.getServerPort();
        hostPort = new HostPort(serverHost, serverPort);

        connectZooKeeper();

        initZnode();
    }

    /**
     * Connect with ZooKeeper with retries.
     *
     * @throws IOException when error to construct ZooKeeper object after retrying
     */
    protected void connectZooKeeper() throws IOException {
        LOG.info("Connecting ZooKeeper " + zkQuorum);

        for (int i = 0; i <= connectRetryTimes; i++) {
            try {
                zooKeeper = new RecoverableZooKeeper(zkQuorum, sessionTimeout, this);
                break;
            } catch (IOException e) {
                if (i == connectRetryTimes) {
                    throw new IOException("Can't connect ZooKeeper after retrying", e);
                }
                LOG.error("Exception to connect ZooKeeper, retry " + (i + 1) + " times");
            }
        }
    }

    /**
     * Initialize the base znodes.
     */
    protected void initZnode() {
        try {
            ZooKeeperUtil.createAndFailSilent(this, TaskManagerConfig.getZkRootPath());
            ZooKeeperUtil.createAndFailSilent(this, baseZnode);
        } catch (Exception e) {
            LOG.fatal("Error to create znode " + baseZnode + ", exit immediately", e);
            System.exit(0);
        }
    }

    /**
     * Override this mothod to deal with events for leader election.
     *
     * @param event the ZooKeeper event
     */
    @Override
    public void process(WatchedEvent event) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received ZooKeeper Event, " + "type=" + event.getType() + ", " + "state=" + event.getState()
                    + ", " + "path=" + event.getPath());
        }

        switch (event.getType()) {
            case None: {
                processConnection(event);
                break;
            }
            case NodeCreated: {
                processNodeCreated(event.getPath());
                break;
            }
            case NodeDeleted: {
                processNodeDeleted(event.getPath());
                break;
            }
            case NodeDataChanged: {
                processDataChanged(event.getPath());
                break;
            }
            case NodeChildrenChanged: {
                processNodeChildrenChanged(event.getPath());
                break;
            }
            default:
                break;
        }
    }

    /**
     * Deal with connection event, exit current process if auth fails or session expires.
     *
     * @param event the ZooKeeper event
     */
    protected void processConnection(WatchedEvent event) {
        switch (event.getState()) {
            case SyncConnected:
                LOG.info(hostPort.getHostPort() + " sync connect from ZooKeeper");
                try {
                    waitToInitZooKeeper(2000); // init zookeeper in another thread, wait for a while
                } catch (Exception e) {
                    LOG.fatal("Error to init ZooKeeper object after sleeping 2000 ms, exit immediately");
                    System.exit(0);
                }
                LOG.info(hostPort.getHostPort() + " init ZooKeeper object successfully, session id is 0x" + Long.toHexString(
                        zooKeeper.getSessionId()));
                connected.set(true);
                break;
            /*
            case Disconnected: // be triggered when kill the server or the leader of zk cluster 
              LOG.warn(hostPort.getHostPort() + " received disconnected from ZooKeeper");
            
              if (becomeActiveServer.get()) {
                // Exit if this is master and disconnect from ZK
                System.exit(0);
              }
              break;
            */
            case AuthFailed:
                LOG.fatal(hostPort.getHostPort() + " auth fail, exit immediately");
                System.exit(0);
            case Expired:
                LOG.warn(hostPort.getHostPort() + " received expired from ZooKeeper");
            default:
                // expired or other unknown state: mark as disconnected
                connected.set(false);
                break;
        }
    }

    /**
     * Deal with create node event, just call the leader election.
     *
     * @param path which znode is created
     */
    protected void processNodeCreated(String path) {
        if (path.equals(masterZnode)) {
            LOG.info(masterZnode + " created and try to become active master");
            handleMasterNodeChange();
        }
    }

    /**
     * Deal with delete node event, just call the leader election.
     *
     * @param path which znode is deleted
     */
    protected void processNodeDeleted(String path) {
        if (path.equals(masterZnode)) {
            LOG.info(masterZnode + " deleted and try to become active master");
            handleMasterNodeChange();
        }
    }

    /**
     * Do nothing when data changes, should be overrided.
     *
     * @param path which znode's data is changed
     */
    protected void processDataChanged(String path) {

    }

    /**
     * Do nothing when children znode changes, should be overrided.
     *
     * @param path which znode's children is changed.
     */
    protected void processNodeChildrenChanged(String path) {

    }

    /**
     * Implement the logic of leader election.
     */
    private void handleMasterNodeChange() {
        try {
            synchronized (hasActiveServer) {
                if (ZooKeeperUtil.watchAndCheckExists(this, masterZnode)) {
                    // A master node exists, there is an active master
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("A master is now available");
                    }
                    hasActiveServer.set(true);
                } else {
                    // Node is no longer there, cluster does not have an active master
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No master available. Notifying waiting threads");
                    }
                    hasActiveServer.set(false);
                    // Notify any thread waiting to become the active master
                    hasActiveServer.notifyAll();
                }
            }
        } catch (KeeperException ke) {
            LOG.error("Received an unexpected KeeperException, aborting", ke);
        }
    }

    /**
     * Implement the logic of server to wait to become active master.
     *
     * @return false if error to wait to become active master
     */
    public boolean blockUntilActive() {
        while (true) {
            try {
                if (ZooKeeperUtil.createEphemeralNodeAndWatch(this, masterZnode, hostPort.getHostPort().getBytes())) {

                    // We are the master, return
                    hasActiveServer.set(true);
                    becomeActiveServer.set(true);
                    LOG.info("Become active master in " + hostPort.getHostPort());
                    return true;
                }

                hasActiveServer.set(true);

                // we start the server with the same ip_port stored in master znode, that means we want to
                // restart the server?
                String msg;
                byte[] bytes = ZooKeeperUtil.getDataAndWatch(this, masterZnode);
                if (bytes == null) {
                    msg = ("A master was detected, but went down before its address "
                            + "could be read.  Attempting to become the next active master");
                } else {
                    if (hostPort.getHostPort().equals(new String(bytes))) {
                        msg = ("Current master has this master's address, " + hostPort.getHostPort()
                                + "; master was restarted? Deleting node.");
                        // Hurry along the expiration of the znode.
                        ZooKeeperUtil.deleteNode(this, masterZnode);
                    } else {
                        msg = "Another master " + new String(bytes) + " is the active master, " + hostPort.getHostPort()
                                + "; waiting to become the next active master";
                    }
                }
                LOG.info(msg);
            } catch (KeeperException ke) {
                LOG.error("Received an unexpected KeeperException when block to become active, aborting", ke);
                return false;
            }

            synchronized (hasActiveServer) {
                while (hasActiveServer.get()) {
                    try {
                        hasActiveServer.wait();
                    } catch (InterruptedException e) {
                        // We expect to be interrupted when a master dies, will fall out if so
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Interrupted while waiting to be master");
                        }
                        return false;
                    }
                }
            }
        }
    }

    /**
     * Close the ZooKeeper object.
     */
    public void close() {
        if (zooKeeper != null) {
            try {
                connected.set(false);
                zooKeeper.close();
            } catch (InterruptedException e) {
                LOG.error("Interrupt when closing zookeeper connection", e);
            }
        }
    }

    /**
     * Wait to init ZooKeeper object, only sleep when it's null.
     *
     * @param maxWaitMillis the max sleep time
     * @throws Exception if ZooKeeper object is still null
     */
    public void waitToInitZooKeeper(long maxWaitMillis) throws Exception {
        long finished = System.currentTimeMillis() + maxWaitMillis;
        while (System.currentTimeMillis() < finished) {
            if (this.zooKeeper != null) {
                return;
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new Exception(e);
            }
        }
        throw new Exception();
    }

    public ZooKeeper getZooKeeper() throws KeeperException {
        return zooKeeper.checkZk(); // return raw zookeeper here
    }

    // we can't know from the zookeeper client whether it's connected or not, so we use a flag to record it
    // Another way is reconnect after get expired exception when get/set from zk
    public void startReconnectThread() {
        // TODO: just create a thread now, if more threads for new tasks, should use thread pool
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(2000);
                        synchronized (connected) {
                            if (connected.get() == false) {
                                LOG.info("Try to reconnect ZooKeeper");
                                zooKeeper.reconnectAfterExpiration();
                                connected.set(true);
                                initZnode(); // test
                                // won't init znode again, because it's already created in init. If znode is deleted, should
                                // restart taskmanager
                                LOG.info("Try to become active master after reconnecting ZooKeeper");
                                blockUntilActive();
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error to reconnect ZooKeeper", e);
                    }
                }
            }
        }).start();
    }
}

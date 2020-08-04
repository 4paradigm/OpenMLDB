package com._4paradigm.rtidb.client.ha.impl;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.*;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com.google.protobuf.InvalidProtocolBufferException;
import io.brpc.client.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rtidb.api.TabletServer;
import rtidb.blobserver.BlobServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RTIDBClusterClient implements Watcher, RTIDBClient {
    private final static Logger logger = LoggerFactory.getLogger(RTIDBClusterClient.class);
    private static Set<String> localIpAddr = new HashSet<String>();
    private final static ScheduledExecutorService clusterGuardThread = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    });
    private volatile ZooKeeper zookeeper;
    private volatile Map<String, TableHandler> name2tables = new TreeMap<String, TableHandler>();
    private volatile Map<Integer, TableHandler> id2tables = new TreeMap<Integer, TableHandler>();
    private RpcBaseClient baseClient;
    private NodeManager nodeManager;
    private RTIDBClientConfig config;
    private Watcher notifyWatcher;
    private AtomicBoolean watching = new AtomicBoolean(true);
    private AtomicBoolean isClose = new AtomicBoolean(false);
    private final String blobPrefix = "blob_";

    public RTIDBClusterClient(RTIDBClientConfig config) {
        this.config = config;
    }

    public void init() throws TabletException, IOException {
        RpcClientOptions options = new RpcClientOptions();
        options.setIoThreadNum(config.getIoThreadNum());
        options.setMaxConnectionNumPerHost(config.getMaxCntCnnPerHost());
        options.setReadTimeoutMillis(config.getReadTimeout());
        options.setWriteTimeoutMillis(config.getWriteTimeout());
        options.setMaxTryTimes(config.getMaxRetryCnt());
        options.setTimerBucketSize(config.getTimerBucketSize());
        baseClient = new RpcBaseClient(options);
        nodeManager = new NodeManager(baseClient);
        getLocalIpAddress();
        notifyWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    refreshNodeList();
                    refreshRouteTable();
                } catch (Exception e) {
                    logger.error("fail to handle zookeeper connected  event", e);
                } finally {
                    try {
                        zookeeper.getData(config.getZkTableNotifyPath(), notifyWatcher, null);
                        watching.set(true);
                    } catch (Exception e) {
                        logger.error("fail to add watch to notify node", e);
                        watching.set(false);
                    }
                }
            }

        };
        isClose.set(false);
        connectToZk();
        onZkConnected();
        tryWatch();
        clusterGuardThread.schedule(new Runnable() {
            @Override
            public void run() {
                checkWatchStatus();

            }
        }, 1, TimeUnit.MINUTES);

    }

    private void connectToZk() throws TabletException, IOException {
        ZooKeeper localZk = new ZooKeeper(config.getZkEndpoints(), (int) config.getZkSesstionTimeout(), this);
        int failedCountDown = 20;
        while (!localZk.getState().isConnected() && failedCountDown > 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
            }
            failedCountDown--;
        }
        if (!localZk.getState().isConnected()) {
            try {
                localZk.close();
            } catch (Exception e) {
                logger.error("fail to close local zookeeper client", e);
            }
            throw new TabletException("fail to connect zookeeper " + config.getZkEndpoints());
        }
        ZooKeeper old = zookeeper;
        if (old != null) {
            try {
                old.close();
                logger.info("close old zookeeper client ok");
            } catch (Exception e) {
                logger.error("fail to close old zookeeper client", e);
            }
        }
        zookeeper = localZk;
        logger.info("switch to new zookeeper client instance ok");
    }

    private void onZkConnected() {
        try {
            refreshNodeList();
            refreshRouteTable();
        } catch (Exception e) {
            logger.error("fail to handle zookeeper connected  event", e);
        }
    }

    private void getLocalIpAddress() {
        try {
            Enumeration e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface n = (NetworkInterface) e.nextElement();
                Enumeration ee = n.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress i = (InetAddress) ee.nextElement();
                    logger.info("ip {} of local host binded ", i.getHostAddress());
                    localIpAddr.add(i.getHostAddress().toLowerCase());
                }
            }
            // get local hostname
            String hostname = InetAddress.getLocalHost().getHostName();
            localIpAddr.add(hostname);
            logger.info("add hostname {} to local ip set ", hostname);
        } catch (Exception e) {
            logger.error("fail to get local ip address", e);
        }
    }

    private void tryWatch() {
        try {
            if (zookeeper == null || !zookeeper.getState().isConnected()) {
                connectToZk();
                onZkConnected();
            }
            zookeeper.getData(config.getZkTableNotifyPath(), notifyWatcher, null);
            watching.set(true);
        } catch (Exception e) {
            logger.error("fail to add watch to notify node and will retry one minute later", e);
            watching.set(false);
        }
    }

    private void checkWatchStatus() {
        if (isClose.get()) {
            return;
        }
        if (!watching.get()) {
            tryWatch();
        }
        clusterGuardThread.schedule(new Runnable() {

            @Override
            public void run() {
                checkWatchStatus();
            }
        }, 1, TimeUnit.MINUTES);
    }

    public void refreshRouteTable() {
        Map<String, TableHandler> oldTables = name2tables;
        Map<String, TableHandler> newTables = new TreeMap<String, TableHandler>();
        Map<Integer, TableHandler> oldid2tables = id2tables;
        Map<Integer, TableHandler> newid2tables = new TreeMap<Integer, TableHandler>();
        try {
            Stat stat = zookeeper.exists(config.getZkTableRootPath(), false);
            if (stat == null) {
                logger.warn("path {} does not exist", config.getZkTableRootPath());
                return;
            }
            List<TableInfo> newTableList = new ArrayList<TableInfo>();
            List<String> children = zookeeper.getChildren(config.getZkTableRootPath(), false);
            for (String path : children) {
                byte[] data = zookeeper.getData(config.getZkTableRootPath() + "/" + path, false, null);
                if (data != null) {
                    try {
                        TableInfo tableInfo = TableInfo.parseFrom(data);
                        newTableList.add(tableInfo);
                    } catch (InvalidProtocolBufferException e) {
                        logger.error("invalid table data with name {}", path, e);
                    }
                }
            }
            HashMap<String, String> realEpMap = new HashMap<>();
            getRealEpMap(realEpMap);

            for (TableInfo table : newTableList) {
                try {
                    TableHandler handler = new TableHandler(table);
                    if (config.getReadStrategies().containsKey(table.getName())) {
                        handler.setReadStrategy(config.getReadStrategies().get(table.getName()));
                    } else {
                        handler.setReadStrategy(config.getGlobalReadStrategies());
                    }
                    PartitionHandler[] partitionHandlerGroup = new PartitionHandler[table.getTablePartitionList().size()];
                    for (TablePartition partition : table.getTablePartitionList()) {
                        PartitionHandler ph = partitionHandlerGroup[partition.getPid()];
                        if (ph == null) {
                            ph = new PartitionHandler();
                            partitionHandlerGroup[partition.getPid()] = ph;
                        }
                        for (PartitionMeta pm : partition.getPartitionMetaList()) {
                            String pmEndpoint = pm.getEndpoint();
                            if (!pm.getIsAlive()) {
                                logger.warn("table {} partition {} with endpoint {} is dead", table.getName(), partition.getPid(), pmEndpoint);
                                continue;
                            }
                            EndPoint endpoint;
                            if (realEpMap.isEmpty()) {
                                endpoint = new EndPoint(pmEndpoint);
                            } else {
                                String ep = realEpMap.get(pmEndpoint);
                                if (ep == null) {
                                    logger.warn("not found {} in realEpMap", pmEndpoint);
                                    continue;
                                }
                                endpoint = new EndPoint(ep);
                            }
                            BrpcChannelGroup bcg = nodeManager.getChannel(endpoint);
                            if (bcg == null) {
                                logger.warn("no alive endpoint for table {}, expect endpoint {}", table.getName(),
                                        endpoint);
                                continue;
                            }
                            SingleEndpointRpcClient client = new SingleEndpointRpcClient(baseClient);
                            client.updateEndpoint(endpoint, bcg);
                            TabletServer ts = (TabletServer) RpcProxy.getProxy(client, TabletServer.class);
                            TabletServerWapper tabletServerWapper = new TabletServerWapper(pmEndpoint, ts);
                            if (pm.getIsLeader()) {
                                ph.setLeader(tabletServerWapper);
                            } else {
                                ph.getFollowers().add(tabletServerWapper);
                            }
                            if (localIpAddr.contains(endpoint.getIp().toLowerCase())) {
                                ph.setFastTablet(tabletServerWapper);
                            }
                        }
                    }
                    if (table.hasTableType()) {
                        List<String> blobs = new ArrayList<String>();
                        if (table.hasBlobInfo()) {
                            NS.BlobInfo blobInfo = table.getBlobInfo();
                            for (NS.BlobPartition part : blobInfo.getBlobPartitionList()) {
                                for (NS.BlobPartitionMeta meta : part.getPartitionMetaList()) {
                                    if (!meta.getIsAlive()) {
                                        continue;
                                    }
                                    blobs.add(meta.getEndpoint());
                                }
                            }
                        }
                        if (blobs.size() > 0) {
                            EndPoint endpoint;
                            String blob = blobs.get(0);
                            if (realEpMap.isEmpty()) {
                                endpoint = new EndPoint(blob);
                            } else {
                                String ep = realEpMap.get(blob);
                                if (ep == null) {
                                    logger.warn("not found {} in realEpMap", blob);
                                    continue;
                                }
                                endpoint = new EndPoint(ep);
                            }
                            BrpcChannelGroup bcg = nodeManager.getChannel(endpoint);
                            if (bcg == null) {
                                logger.warn("no alive endpoint for table {}, expect endpoint {}", table.getName(),
                                        endpoint);
                            } else {
                                SingleEndpointRpcClient client = new SingleEndpointRpcClient(baseClient);
                                client.updateEndpoint(endpoint, bcg);
                                BlobServer bs = (BlobServer) RpcProxy.getProxy(client, BlobServer.class);
                                handler.setBlobServer(bs);
                            }
                        }
                    }

                    handler.setPartitions(partitionHandlerGroup);
                    newTables.put(table.getName(), handler);
                    newid2tables.put(table.getTid(), handler);
                } catch (Exception e) {
                    logger.warn("refresh table {} failed", table.getName());
                }
            }
            // swap
            name2tables = newTables;
            id2tables = newid2tables;
            oldTables.clear();
            oldid2tables.clear();
        } catch (Exception e) {
            logger.error("fail to refresh table", e);
        }
    }

    public boolean refreshNodeList() {
        try {
            Stat stat = zookeeper.exists(config.getZkNodeRootPath(), false);
            if (stat == null) {
                logger.warn("path {} does not exist", config.getZkNodeRootPath());
                return true;
            }
            HashMap<String, String> realEpMap = new HashMap<>();
            getRealEpMap(realEpMap);
            Set<EndPoint> endpoinSet = new HashSet<EndPoint>();
            List<String> children = zookeeper.getChildren(config.getZkNodeRootPath(), false);
            for (String path : children) {
                if (path.isEmpty()) {
                    continue;
                }
                if (path.startsWith(blobPrefix)) {
                    path = path.substring(blobPrefix.length());
                }
                logger.debug("alive endpoint {}", path);
                try {
                    if (realEpMap.isEmpty()) {
                        endpoinSet.add(new EndPoint(path));
                    } else {
                        String ep = realEpMap.get(path);
                        if (ep == null) {
                            logger.warn("not found {} in realEpMap", path);
                            continue;
                        }
                        endpoinSet.add(new EndPoint(ep));
                    }
                } catch (Exception e) {
                    logger.error("fail to add endpoint", e);
                }
            }
            nodeManager.update(endpoinSet);
            return true;
        } catch (Exception e) {
            logger.error("fail to refresh node manger", e);
        }
        return false;
    }

    public TableHandler getHandler(String name) {
        return name2tables.get(name);
    }

    public TableHandler getHandler(int id) {
        return id2tables.get(id);
    }

    @Override
    public void process(WatchedEvent event) {
    }

    @Override
    public void close() {
        isClose.set(true);
        // static members need not shutdown
        // clusterGuardThread.shutdown();
        if (zookeeper != null) {
            try {
                zookeeper.close();
            } catch (InterruptedException e) {
                logger.error("fail to close zk", e);
            }
        }
        if (nodeManager != null) {
            try {

                nodeManager.close();
            } catch (Exception e) {
                logger.error("fail to close node manager", e);
            }
        }
        if (baseClient != null) {
            try {
                baseClient.stop();
            } catch (Exception e) {
                logger.error("fail to close base client", e);
            }
        }
    }

    private void getRealEpMap(HashMap<String, String> realEpMap) throws KeeperException, InterruptedException {
        // get sdkendpoint
        if (zookeeper.exists(config.getZkSdkEndpointPath(), false) != null) {
            List<String> sdkEndpoints = zookeeper.getChildren(config.getZkSdkEndpointPath(), false);
            for (String path : sdkEndpoints) {
                if (path.isEmpty()) {
                    continue;
                }
                logger.debug("alive sdkendpoint {}", path);
                byte[] data = zookeeper.getData(config.getZkSdkEndpointPath() + "/" + path, false, null);
                if (data != null) {
                    realEpMap.put(path, new String(data, Charset.forName("UTF-8")));
                }
            }
        }
        if (realEpMap.isEmpty() && zookeeper.exists(config.getZkServerNamePath(), false) != null) {
            // get real endpoint
            List<String> serverNames = zookeeper.getChildren(config.getZkServerNamePath(), false);
            for (String path : serverNames) {
                if (path.isEmpty()) {
                    continue;
                }
                logger.debug("alive server name {}", path);
                byte[] data = zookeeper.getData(config.getZkServerNamePath() + "/" + path, false, null);
                if (data != null) {
                    realEpMap.put(path, new String(data, Charset.forName("UTF-8")));
                }
            }
        }
    }

    @Override
    public RTIDBClientConfig getConfig() {
        return config;
    }

}

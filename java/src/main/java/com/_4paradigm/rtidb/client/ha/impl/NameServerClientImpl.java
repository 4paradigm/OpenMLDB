package com._4paradigm.rtidb.client.ha.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.NameServerClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.ns.NS.ChangeLeaderRequest;
import com._4paradigm.rtidb.ns.NS.CreateTableRequest;
import com._4paradigm.rtidb.ns.NS.DropTableRequest;
import com._4paradigm.rtidb.ns.NS.GeneralResponse;
import com._4paradigm.rtidb.ns.NS.RecoverEndpointRequest;
import com._4paradigm.rtidb.ns.NS.ShowTableRequest;
import com._4paradigm.rtidb.ns.NS.ShowTableResponse;
import com._4paradigm.rtidb.ns.NS.ShowTabletRequest;
import com._4paradigm.rtidb.ns.NS.ShowTabletResponse;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TabletStatus;
import com.sun.org.apache.regexp.internal.recompile;

import io.brpc.client.BrpcChannelGroup;
import io.brpc.client.EndPoint;
import io.brpc.client.RpcBaseClient;
import io.brpc.client.RpcProxy;
import io.brpc.client.SingleEndpointRpcClient;
import io.brpc.client.RpcClientOptions;
import rtidb.nameserver.NameServer;

public class NameServerClientImpl implements NameServerClient, Watcher {
    private final static Logger logger = LoggerFactory.getLogger(NameServerClientImpl.class);
    private String zkEndpoints;
    private String leaderPath;
    private volatile ZooKeeper zookeeper;
    private SingleEndpointRpcClient rpcClient;
    private volatile NameServer ns;
    private Watcher notifyWatcher;
    private AtomicBoolean watching = new AtomicBoolean(true);
    private AtomicBoolean isClose = new AtomicBoolean(false);
    private RTIDBClientConfig config;
    private final static ScheduledExecutorService clusterGuardThread = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    });

    public NameServerClientImpl(String zkEndpoints, String leaderPath, RTIDBClientConfig config) {
        this.zkEndpoints = zkEndpoints;
        this.leaderPath = leaderPath;
        this.config = config;
    }

    public NameServerClientImpl(String zkEndpoints, String leaderPath) {
        this.zkEndpoints = zkEndpoints;
        this.leaderPath = leaderPath;
        this.config = null;
    }

    public void setConfig(RTIDBClientConfig config) {
        this.config = config;
        return;
    }

    @Deprecated
    public NameServerClientImpl(String endpoint) {
        EndPoint addr = new EndPoint(endpoint);
        RpcBaseClient bs = new RpcBaseClient();
        rpcClient = new SingleEndpointRpcClient(bs);
        BrpcChannelGroup bcg = new BrpcChannelGroup(addr.getIp(), addr.getPort(),
                bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap());
        rpcClient.updateEndpoint(addr, bcg);
        ns = (NameServer) RpcProxy.getProxy(rpcClient, NameServer.class);
    }

    public void init() throws Exception {
        isClose.set(false);
        if (config != null) {
            RpcClientOptions options = new RpcClientOptions();
            options.setIoThreadNum(config.getIoThreadNum());
            options.setMaxConnectionNumPerHost(config.getMaxCntCnnPerHost());
            options.setReadTimeoutMillis(config.getReadTimeout());
            options.setWriteTimeoutMillis(config.getWriteTimeout());
            options.setMaxTryTimes(config.getMaxRetryCnt());
            options.setTimerBucketSize(config.getTimerBucketSize());
        }
        notifyWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("zookeeper leader node has changed");
                try {
                    connectNS();
                } catch (Exception e) {
                    logger.error("fail to handle zookeeper node update event", e);
                } finally {
                    try {
                        zookeeper.getChildren(leaderPath, notifyWatcher);
                        watching.set(true);
                    } catch (Exception e) {
                        logger.error("fail to add watch to notify node", e);
                        watching.set(false);
                    }
                }
            }
        };
        connectZk();
        connectNS();
        tryWatch();
        clusterGuardThread.schedule(new Runnable() {
            @Override
            public void run() {
                checkWatchStatus();
            }
        }, 1, TimeUnit.MINUTES);
    }

    private void connectZk() throws TabletException, IOException {
        ZooKeeper localZk = new ZooKeeper(zkEndpoints, 10000, this);
        int tryCnt = 10;
        while (!localZk.getState().isConnected() && tryCnt > 0) {
            try {
                Thread.sleep(1000);
            }catch(InterruptedException e) {
                logger.error("interrupted", e);
            }
            tryCnt --;
        }
        if (!localZk.getState().isConnected()) {
            throw new TabletException("fail to connect to zookeeper " + zkEndpoints);
        }
        ZooKeeper old = zookeeper;
        if (old != null) {
            try {
                old.close();
                logger.info("close old zookeeper client ok");
            }catch(Exception e) {
                logger.error("fail to close old zookeeper client", e);
            }
        }
        zookeeper = localZk;
        logger.info("switch to new zookeeper client instance ok");
    }

    private void connectNS() throws Exception {
        List<String> children = zookeeper.getChildren(leaderPath, false);
        if (children.isEmpty()) {
            throw new TabletException("no nameserver avaliable");
        }
        Collections.sort(children);
        byte[] bytes = zookeeper.getData(leaderPath + "/" + children.get(0), false, null);
        EndPoint endpoint = new EndPoint(new String(bytes));
        RpcBaseClient bs = new RpcBaseClient();
        if (rpcClient != null) {
            rpcClient.stop();
        }
        rpcClient = new SingleEndpointRpcClient(bs);
        BrpcChannelGroup bcg = new BrpcChannelGroup(endpoint.getIp(), endpoint.getPort(),
                bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap());
        rpcClient.updateEndpoint(endpoint, bcg);
        ns = (NameServer) RpcProxy.getProxy(rpcClient, NameServer.class);
        logger.info("connect leader path {} endpoint {} ok", children.get(0), endpoint);
    }

    private void tryWatch() {
        try {
            if (zookeeper == null || !zookeeper.getState().isConnected()) {
                connectZk();
                connectNS();
            }
            zookeeper.getChildren(leaderPath, notifyWatcher);
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
    
    @Override
    public boolean createTable(TableInfo tableInfo) {
        CreateTableRequest request = CreateTableRequest.newBuilder().setTableInfo(tableInfo).build();
        GeneralResponse response = ns.createTable(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        logger.warn("fail to create table for error {}", response.getMsg());
        return false;
    }

    @Override
    public boolean dropTable(String tname) {
        DropTableRequest request = DropTableRequest.newBuilder().setName(tname).build();
        GeneralResponse response = ns.dropTable(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        logger.warn("fail to drop table for error {}", response.getMsg());
        return false;
    }

    @Override
    public List<TableInfo> showTable(String tname) {
        ShowTableRequest request = null;
        if (tname == null || tname.isEmpty()) {
            request = ShowTableRequest.newBuilder().build();
        } else {
            request = ShowTableRequest.newBuilder().setName(tname).build();
        }
        ShowTableResponse response = ns.showTable(request);
        return response.getTableInfoList();
    }
    
    @Override
    public void process(WatchedEvent event) {
        
        
    }
    
    public void close() {
        isClose.set(true);
        try {
            if (zookeeper != null) {
                zookeeper.close();
            }
        }catch(Exception e) {
            logger.error("fail to close zookeeper", e);
        }
        if (rpcClient != null) {
            rpcClient.stop();
        }
    }

    @Override
    public boolean changeLeader(String tname, int pid) {
        ChangeLeaderRequest request = ChangeLeaderRequest.newBuilder().setName(tname).setPid(pid).build();
        GeneralResponse response = ns.changeLeader(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean recoverEndpoint(String endpoint) {
        RecoverEndpointRequest request = RecoverEndpointRequest.newBuilder().setEndpoint(endpoint).build();
        GeneralResponse response = ns.recoverEndpoint(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public List<String> showTablet() {
        ShowTabletRequest request = ShowTabletRequest.newBuilder().build();
        ShowTabletResponse response = ns.showTablet(request);
        List<String> tablets = new ArrayList<String>();
        for (TabletStatus ts : response.getTabletsList()) {
            tablets.add(ts.getEndpoint());
        }
        return tablets;
    }

    public Map<String,String> showNs() throws Exception {
        List<String> node = zookeeper.getChildren(leaderPath,false);
        Map<String,String> nsEndpoint = new HashMap<>();
        int i = 0;
        if (node.isEmpty()) {
            return nsEndpoint;
        }
        Collections.sort(node);
        for (String e: node) {
            byte[] bytes = zookeeper.getData(leaderPath + "/" + e, false, null);
            String endpoint = new String(bytes);
            if (!nsEndpoint.containsKey(endpoint)) {
                if (i == 0) {
                    nsEndpoint.put(endpoint, "leader");
                    i++;
                } else {
                    nsEndpoint.put(endpoint, "standby");
                }
            }
        }
        return nsEndpoint;
    }
}

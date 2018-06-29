package com._4paradigm.rtidb.client.ha.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.NameServerClient;
import com._4paradigm.rtidb.client.TabletException;
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

import io.brpc.client.BrpcChannelGroup;
import io.brpc.client.EndPoint;
import io.brpc.client.RpcBaseClient;
import io.brpc.client.RpcProxy;
import io.brpc.client.SingleEndpointRpcClient;
import rtidb.nameserver.NameServer;

public class NameServerClientImpl implements NameServerClient, Watcher {
    private final static Logger logger = LoggerFactory.getLogger(NameServerClientImpl.class);
    private String zkEndpoints;
    private String leaderPath;
    private ZooKeeper zookeeper;
    private SingleEndpointRpcClient rpcClient;
    private NameServer ns;

    public NameServerClientImpl(String zkEndpoints, String leaderPath) {
        this.zkEndpoints = zkEndpoints;
        this.leaderPath = leaderPath;
    }
    
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

        zookeeper = new ZooKeeper(zkEndpoints, 10000, this);
        int tryCnt = 10;
        while (!zookeeper.getState().isConnected() && tryCnt > 0) {
            try {
                Thread.sleep(1000);
            }catch(InterruptedException e) {
                logger.error("interrupted", e);
            }
            tryCnt --;
        }
        if (!zookeeper.getState().isConnected()) {
            throw new TabletException("fail to connect to zookeeper " + zkEndpoints);
        }
        List<String> children = zookeeper.getChildren(leaderPath, false);
        if (children.isEmpty()) {
            throw new TabletException("no nameserver avaliable");
        }
        Collections.sort(children);
        byte[] bytes = zookeeper.getData(leaderPath + "/" + children.get(0), false, null);
        EndPoint endpoint = new EndPoint(new String(bytes));
        RpcBaseClient bs = new RpcBaseClient();
        rpcClient = new SingleEndpointRpcClient(bs);
        BrpcChannelGroup bcg = new BrpcChannelGroup(endpoint.getIp(), endpoint.getPort(),
                bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap());
        rpcClient.updateEndpoint(endpoint, bcg);
        ns = (NameServer) RpcProxy.getProxy(rpcClient, NameServer.class);
        logger.info("connect leader path {} endpoint {} ok", children.get(0), endpoint);

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
        try {
            if (zookeeper != null) {
                zookeeper.close();
            }
        }catch(Exception e) {
            logger.error("fail to close zookeeper", e);
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
    

}

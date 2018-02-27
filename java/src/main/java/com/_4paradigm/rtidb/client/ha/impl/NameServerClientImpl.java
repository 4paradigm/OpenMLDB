package com._4paradigm.rtidb.client.ha.impl;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.NameServerClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.ns.NS.CreateTableRequest;
import com._4paradigm.rtidb.ns.NS.DropTableRequest;
import com._4paradigm.rtidb.ns.NS.GeneralResponse;
import com._4paradigm.rtidb.ns.NS.ShowTableRequest;
import com._4paradigm.rtidb.ns.NS.ShowTableResponse;
import com._4paradigm.rtidb.ns.NS.TableInfo;

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

    public void init() throws Exception {

        zookeeper = new ZooKeeper(zkEndpoints, 10000, this);
        while (!zookeeper.getState().isConnected()) {
            Thread.sleep(1000);
        }
        List<String> children = zookeeper.getChildren(leaderPath, false);
        if (children.isEmpty()) {
            throw new TabletException("no nameserver avaliable");
        }
        byte[] bytes = zookeeper.getData(leaderPath + "/" + children.get(0), false, null);
        EndPoint endpoint = new EndPoint(new String(bytes));
        RpcBaseClient bs = new RpcBaseClient();
        rpcClient = new SingleEndpointRpcClient(bs);
        BrpcChannelGroup bcg = new BrpcChannelGroup(endpoint.getIp(), endpoint.getPort(),
                bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap());
        rpcClient.updateEndpoint(endpoint, bcg);
        ns = (NameServer) RpcProxy.getProxy(rpcClient, NameServer.class);
        logger.info("connect leader {} ok", endpoint);

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
            request = ShowTableRequest.newBuilder().build();
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
            
        }
        
    }

}

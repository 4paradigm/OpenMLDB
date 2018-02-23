package com._4paradigm.rtidb.client.ha.impl;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.NameServerClient;
import com._4paradigm.rtidb.ns.NS.CreateTableRequest;
import com._4paradigm.rtidb.ns.NS.DropTableRequest;
import com._4paradigm.rtidb.ns.NS.GeneralResponse;
import com._4paradigm.rtidb.ns.NS.TableInfo;

import io.brpc.client.BrpcChannelGroup;
import io.brpc.client.EndPoint;
import io.brpc.client.RpcBaseClient;
import io.brpc.client.RpcProxy;
import io.brpc.client.SingleEndpointRpcClient;
import rtidb.nameserver.NameServer;

public class NameServerClientImpl implements NameServerClient {
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
    
    public void init() {
        try {
            zookeeper = new ZooKeeper(zkEndpoints, 10000, null);
            while (!zookeeper.getState().isConnected()) {
                Thread.sleep(1000);
            }
            byte[] bytes = zookeeper.getData(leaderPath, false, null);
            EndPoint endpoint = new EndPoint(new String(bytes));
            RpcBaseClient bs = new RpcBaseClient();
            rpcClient = new SingleEndpointRpcClient(bs);
            BrpcChannelGroup bcg =  new BrpcChannelGroup(endpoint.getIp(), endpoint.getPort(),
                    bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap());
            rpcClient.updateEndpoint(endpoint, bcg);
            ns = (NameServer) RpcProxy.getProxy(rpcClient, NameServer.class);
            logger.info("connect leader {} ok", endpoint);
        } catch (Exception e) {
            logger.error("fail to init name server client", e);
        }
    }
    
    @Override
    public boolean createTable(TableInfo tableInfo) {
        CreateTableRequest request = CreateTableRequest.newBuilder().setTableInfo(tableInfo).build();
        GeneralResponse response = ns.createTable(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean dropTable(String tname) {
        DropTableRequest request = DropTableRequest.newBuilder().setName(tname).build();
        GeneralResponse response = ns.dropTable(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

}

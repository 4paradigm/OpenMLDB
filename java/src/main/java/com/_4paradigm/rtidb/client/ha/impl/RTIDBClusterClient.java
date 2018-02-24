package com._4paradigm.rtidb.client.ha.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.ha.NodeManager;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com.google.protobuf.InvalidProtocolBufferException;

import io.brpc.client.BrpcChannelGroup;
import io.brpc.client.EndPoint;
import io.brpc.client.RpcBaseClient;
import io.brpc.client.RpcClientOptions;
import io.brpc.client.RpcProxy;
import io.brpc.client.SingleEndpointRpcClient;
import rtidb.api.TabletServer;

public class RTIDBClusterClient implements Watcher, RTIDBClient {
    private final static Logger logger = LoggerFactory.getLogger(RTIDBClusterClient.class);
    private ZooKeeper zookeeper;
    private volatile Map<String, TableHandler> name2tables = new HashMap<String, TableHandler>();
    private volatile Map<Integer, TableHandler> id2tables = new HashMap<Integer, TableHandler>();
    private RpcBaseClient baseClient;
    private NodeManager nodeManager;
    private RTIDBClientConfig config;

    public RTIDBClusterClient(RTIDBClientConfig config) {
        this.config = config;
    }

    public void init() throws Exception {
        RpcClientOptions options = new RpcClientOptions();
        options.setIoThreadNum(config.getIoThreadNum());
        options.setMaxConnectionNumPerHost(config.getMaxCntCnnPerHost());
        options.setReadTimeoutMillis(config.getReadTimeout());
        options.setWriteTimeoutMillis(config.getWriteTimeout());
        baseClient = new RpcBaseClient(options);
        nodeManager = new NodeManager(baseClient);
        zookeeper = new ZooKeeper(config.getZkEndpoints(), (int) config.getZkSesstionTimeout(), this);
        try {
            while (!zookeeper.getState().isConnected()) {
                Thread.sleep(1000);
            }
            onZkConnected();
        } catch (Exception e) {
            logger.error("fail to init rtidb client", e);
        }

    }

    private void watchNotifier() {

    }

    private void onZkConnected() {
        try {
            refreshNodeList();
            refreshRouteTable();
        } catch (Exception e) {
            logger.error("fail to handle zookeeper connected  event", e);
        }
    }

    public void refreshRouteTable() {
        Map<String, TableHandler> oldTables = name2tables;
        Map<String, TableHandler> newTables = new HashMap<String, TableHandler>();
        try {
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

            for (TableInfo table : newTableList) {
                TableHandler handler = new TableHandler(table);
                PartitionHandler[] partitionHandlerGroup = new PartitionHandler[table.getTablePartitionList().size()];
                for (TablePartition partition : table.getTablePartitionList()) {
                    PartitionHandler ph = partitionHandlerGroup[partition.getPid()];
                    if (ph == null) {
                        ph = new PartitionHandler();
                        partitionHandlerGroup[partition.getPid()] = ph;
                    }
                    for (PartitionMeta pm : partition.getPartitionMetaList()) {
                        EndPoint endpoint = new EndPoint(pm.getEndpoint());
                        BrpcChannelGroup bcg = nodeManager.getChannel(endpoint);
                        if (bcg == null) {
                            logger.warn("no alive endpoint for table {}, expect endpoint {}", table.getName(), endpoint);
                            continue;
                        }
                        SingleEndpointRpcClient client = new SingleEndpointRpcClient(baseClient);
                        client.updateEndpoint(endpoint, bcg);
                        if (pm.getIsLeader()) {
                            ph.setLeader((TabletServer) RpcProxy.getProxy(client, TabletServer.class));
                        } else {
                            ph.getFollowers().add((TabletServer) RpcProxy.getProxy(client, TabletServer.class));
                        }
                    }
                }
                handler.setPartitions(partitionHandlerGroup);
                newTables.put(table.getName(), handler);
            }
            // swap
            name2tables = newTables;
            Map<Integer, TableHandler> oldid2tables = id2tables;
            Map<Integer, TableHandler> newid2tables = new HashMap<Integer, TableHandler>();
            Iterator<Map.Entry<String, TableHandler>> it = newTables.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, TableHandler> entry = it.next();
                newid2tables.put(entry.getValue().getTableInfo().getTid(), entry.getValue());
            }
            id2tables = newid2tables;
            oldTables.clear();
            oldid2tables.clear();
        } catch (Exception e) {
            logger.error("fail to refresh table", e);
        }
    }


    public boolean refreshNodeList() {
        try {
            Set<EndPoint> endpoinSet = new HashSet<EndPoint>();
            List<String> children = zookeeper.getChildren(config.getZkNodeRootPath(), false);
            for (String path : children) {
                if (path.isEmpty()) {
                    continue;
                }
                logger.info("alive endpoint {}", path);
                String[] parts = path.split(":");
                if (parts.length != 2) {
                    logger.warn("invalid endpoint {}", path);
                    continue;
                }
                try {
                    endpoinSet.add(new EndPoint(parts[0], Integer.parseInt(parts[1])));
                } catch (Exception e) {
                    logger.error("fail to add endpoint", e);
                }
            }
            nodeManager.swap(endpoinSet);
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
    public void process(WatchedEvent event) {}
    
}

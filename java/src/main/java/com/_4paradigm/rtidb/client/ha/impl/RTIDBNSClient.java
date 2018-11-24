package com._4paradigm.rtidb.client.ha.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import io.brpc.client.BrpcChannelGroup;
import io.brpc.client.EndPoint;
import io.brpc.client.RpcBaseClient;
import io.brpc.client.RpcClientOptions;
import io.brpc.client.RpcProxy;
import io.brpc.client.SingleEndpointRpcClient;
import rtidb.api.TabletServer;

@Deprecated
public class RTIDBNSClient implements RTIDBClient{

    private static Set<String> localIpAddr = new HashSet<String>();
    private final static Logger logger = LoggerFactory.getLogger(RTIDBNSClient.class);
    private volatile Map<String, TableHandler> name2tables = new HashMap<String, TableHandler>();
    private volatile Map<Integer, TableHandler> id2tables = new HashMap<Integer, TableHandler>();
    private RTIDBClientConfig config;
    private NameServerClientImpl ns;
    private NodeManager nodeManager;
    private RpcBaseClient baseClient;
    public RTIDBNSClient(RTIDBClientConfig config) {
        this.config = config;
        RpcClientOptions options = new RpcClientOptions();
        options.setIoThreadNum(config.getIoThreadNum());
        options.setMaxConnectionNumPerHost(config.getMaxCntCnnPerHost());
        options.setReadTimeoutMillis(config.getReadTimeout());
        options.setWriteTimeoutMillis(config.getWriteTimeout());
        options.setMaxTryTimes(config.getMaxRetryCnt());
        baseClient = new RpcBaseClient(options);
        nodeManager = new NodeManager(baseClient);
        ns = new NameServerClientImpl(config.getNsEndpoint());
        refreshNodeList();
        List<TableInfo> tables = ns.showTable(null);
        refreshRouteTable(tables);
    }

    public TableHandler getHandler(String name) {
        return name2tables.get(name);
    }

    public TableHandler getHandler(int id) {
        return id2tables.get(id);
    }

    @Override
    public RTIDBClientConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        if (nodeManager != null) {
            nodeManager.close();
        }
        if (baseClient != null) {
            baseClient.stop();
        }
    }

    public void refreshRouteTable(List<TableInfo> newTableList) {
        Map<String, TableHandler> oldTables = name2tables;
        Map<String, TableHandler> newTables = new HashMap<String, TableHandler>();
        Map<Integer, TableHandler> oldid2tables = id2tables;
        Map<Integer, TableHandler> newid2tables = new HashMap<Integer, TableHandler>();
        try {
            for (TableInfo table : newTableList) {
                TableHandler handler = new TableHandler(table);
                if (config.getReadStrategies().containsKey(table.getName())) {
                    handler.setReadStrategy(config.getReadStrategies().get(table.getName()));
                }
                PartitionHandler[] partitionHandlerGroup = new PartitionHandler[table.getTablePartitionList().size()];
                for (TablePartition partition : table.getTablePartitionList()) {
                    PartitionHandler ph = partitionHandlerGroup[partition.getPid()];
                    if (ph == null) {
                        ph = new PartitionHandler();
                        partitionHandlerGroup[partition.getPid()] = ph;
                    }
                    for (PartitionMeta pm : partition.getPartitionMetaList()) {
                        if (!pm.getIsAlive()) {
                            logger.warn("table {} partition with endpoint {} is dead", table.getName(), pm.getEndpoint());
                            continue;
                        }
                        EndPoint endpoint = new EndPoint(pm.getEndpoint());
                        BrpcChannelGroup bcg = nodeManager.getChannel(endpoint);
                        if (bcg == null) {
                            logger.warn("no alive endpoint for table {}, expect endpoint {}", table.getName(),
                                    endpoint);
                            continue;
                        }
                        SingleEndpointRpcClient client = new SingleEndpointRpcClient(baseClient);
                        client.updateEndpoint(endpoint, bcg);
                        TabletServer ts = (TabletServer) RpcProxy.getProxy(client, TabletServer.class);
                        if (pm.getIsLeader()) {
                            ph.setLeader(ts);
                        } else {
                            ph.getFollowers().add(ts);
                        }
                        if (localIpAddr.contains(endpoint.getIp().toLowerCase())) {
                            ph.setFastTablet(ts);
                            logger.info("find fast tablet[{}] server for table {} local read", endpoint, table.getName());
                        }
                    }
                }
                handler.setPartitions(partitionHandlerGroup);
                newTables.put(table.getName(), handler);
                newid2tables.put(table.getTid(), handler);
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
            Set<EndPoint> endpoinSet = new HashSet<EndPoint>();
            List<String> children = ns.showTablet();
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
            nodeManager.update(endpoinSet);
            return true;
        } catch (Exception e) {
            logger.error("fail to refresh node manger", e);
        }
        return false;
    }
}

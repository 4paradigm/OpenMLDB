package com._4paradigm.rtidb.client.ha;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.brpc.client.BrpcChannelGroup;
import io.brpc.client.EndPoint;
import io.brpc.client.RpcBaseClient;

public class NodeManager {
    private final static Logger logger = LoggerFactory.getLogger(NodeManager.class);
    private volatile Map<EndPoint, BrpcChannelGroup> endpoints = new HashMap<EndPoint, BrpcChannelGroup>();
    private RpcBaseClient bs;

    public NodeManager(RpcBaseClient bs) {
        this.bs = bs;
    }

    public BrpcChannelGroup getChannel(EndPoint endpoint) {
        return endpoints.get(endpoint);
    }

    public Set<EndPoint> getEndpointSet() {
        return endpoints.keySet();
    }

    public void update(Set<EndPoint> aliveEndpointSet) {
        update(aliveEndpointSet, new HashMap<String, String>());
    }

    public void update(Set<EndPoint> aliveEndpointSet, HashMap<String, String> realEpMap) {
        Map<EndPoint, BrpcChannelGroup> oldEndpoints = endpoints;
        Set<EndPoint> oldEndpointSet = oldEndpoints.keySet();
        // new add endpoint
        Map<EndPoint, BrpcChannelGroup> newEndpoints = new HashMap<EndPoint, BrpcChannelGroup>();
        Iterator<EndPoint> it = aliveEndpointSet.iterator();
        while (it.hasNext()) {
            EndPoint endpoint = it.next();
            if (!oldEndpoints.containsKey(endpoint)) {
                // new add endpoint
                String realEp = endpoint.getIp();
                if (realEpMap != null && !realEpMap.isEmpty() && realEpMap.containsKey(endpoint.getIp())) {
                    realEp = realEpMap.get(endpoint.getIp());
                }
                newEndpoints.put(endpoint, new BrpcChannelGroup(realEp, endpoint.getPort(),
                        bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap()));
                logger.info("add new alive endpoint ip:{} port:{}", endpoint.getIp(), endpoint.getPort());
            } else {
                // reuse old endpoint channel
                if (realEpMap == null || realEpMap.isEmpty() || !realEpMap.containsKey(endpoint.getIp())) {
                    newEndpoints.put(endpoint, oldEndpoints.get(endpoint));
                } else {
                    String realEp = realEpMap.get(endpoint.getIp());
                    if (oldEndpoints.get(endpoint).getIp().equals(realEp)) {
                        newEndpoints.put(endpoint, oldEndpoints.get(endpoint));
                    } else {
                        newEndpoints.put(endpoint, new BrpcChannelGroup(realEp, endpoint.getPort(),
                                bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap()));
                    }
                }
            }
        }

        // swap
        endpoints = newEndpoints;
        // close dead endpoint
        it = oldEndpointSet.iterator();
        while (it.hasNext()) {
            EndPoint endpoint = it.next();
            if (aliveEndpointSet.contains(endpoint)) {
                continue;
            }
            // release dead endpoint resource
            oldEndpoints.get(endpoint).close();
            logger.warn("close dead endpoint {}", endpoint);
        }
        oldEndpoints.clear();
    }
    
    public void close() {
        Iterator<Map.Entry<EndPoint, BrpcChannelGroup>> it = endpoints.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<EndPoint, BrpcChannelGroup> entry = it.next();
            entry.getValue().close();
        }
    }

}

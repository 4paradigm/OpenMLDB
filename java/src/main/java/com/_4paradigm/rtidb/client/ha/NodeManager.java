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
        Map<EndPoint, BrpcChannelGroup> oldEndpoints = endpoints;
        Set<EndPoint> oldEndpointSet = oldEndpoints.keySet();
        // new add endpoint
        Map<EndPoint, BrpcChannelGroup> newEndpoints = new HashMap<EndPoint, BrpcChannelGroup>();
        Iterator<EndPoint> it = aliveEndpointSet.iterator();
        while (it.hasNext()) {
            EndPoint endpoint = it.next();
            if (!oldEndpoints.containsKey(endpoint)) {
                // new add endpoint
                newEndpoints.put(endpoint, new BrpcChannelGroup(endpoint.getIp(), endpoint.getPort(),
                        bs.getRpcClientOptions().getMaxConnectionNumPerHost(), bs.getBootstrap()));
                logger.info("add new alive endpoint ip:{} port:{}", endpoint.getIp(), endpoint.getPort());
            } else {
                // reuse old endpoint channel
                newEndpoints.put(endpoint, oldEndpoints.get(endpoint));
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

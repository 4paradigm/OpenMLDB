package com._4paradigm.rtidb.client.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.Tablet;
import com._4paradigm.rtidb.Tablet.GetTableStatusResponse;
import com._4paradigm.rtidb.Tablet.PutResponse;
import com._4paradigm.rtidb.Tablet.TableStatus;
import com._4paradigm.rtidb.client.TabletAsyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.utils.IpAddressUtils;
import com.google.protobuf.RpcCallback;

public class TabletHaAsyncClient implements TabletAsyncClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletHaAsyncClient.class);
    private volatile Map<Integer, TableLocator> locators = new HashMap<Integer, TableLocator>();
    private List<TabletEndpoint> endpoints;
    private Map<TabletEndpoint, TabletAsyncClientImpl> clients = new HashMap<TabletEndpoint, TabletAsyncClientImpl>();
    private int maxFrameLength;
    private int eventLoopThreadCnt;
    public TabletHaAsyncClient(List<TabletEndpoint> endpoints,
            int maxFrameLength, int eventLoopThreadCnt){
        this.endpoints = endpoints;
        this.maxFrameLength = maxFrameLength;
        this.eventLoopThreadCnt = eventLoopThreadCnt;
    }
    
    public void init() throws InterruptedException {
        for (TabletEndpoint endpoint : endpoints) {
            TabletAsyncClientImpl client = new TabletAsyncClientImpl(endpoint.getHost(),
                    endpoint.getPort(), maxFrameLength, eventLoopThreadCnt);
            client.init();
            clients.put(endpoint, client);
        }
        refreshLocator();
    }
    
    public void refreshLocator() {
        Map<Integer, TableLocator> newLocators = new HashMap<Integer, TableLocator>();
        Iterator<Map.Entry<TabletEndpoint, TabletAsyncClientImpl>> it = clients.entrySet().iterator();
        final CountDownLatch latch = new CountDownLatch(clients.size());
        final GetTableStatusResponse[] responses = new GetTableStatusResponse[clients.size()];
        final TabletEndpoint[] clientEndpoints = new TabletEndpoint[clients.size()];
        int index = 0;
        while (it.hasNext()) {
            final int holder = index;
            final Map.Entry<TabletEndpoint, TabletAsyncClientImpl> entry = it.next();
            clientEndpoints[holder] = entry.getKey();
            entry.getValue().getTables(new RpcCallback<Tablet.GetTableStatusResponse>() {
                @Override
                public void run(GetTableStatusResponse parameter) {
                    responses[holder] = parameter;
                    latch.countDown();
                }
            });
            index++;
        }
        try {
            latch.await(10 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("refresh locator interrupted", e);
            
        }
        
        if (latch.getCount() > 0) {
            logger.error("refresh locator timeout");
        }
        
        for (int i = 0; i < responses.length; i++) {
            GetTableStatusResponse response = responses[i];
            if (response == null || response.getCode() != 0) {
                continue;
            }
            TabletEndpoint endpoint = clientEndpoints[i];
            for (TableStatus status : response.getAllTableStatusList()) {
                TableLocator locator =newLocators.get(status.getTid());
                if (locator == null) {
                    locator = new TableLocator();
                    locator.setTid(status.getTid());
                    newLocators.put(status.getTid(), locator);
                }
                
                if (locator.getPartitions().length <= status.getPid()) {
                    throw new TabletException("table partition id is greater than 63");
                }
                
                PartitionLocator partition = locator.getPartitions()[status.getPid()];
                if (partition == null) {
                    partition = new PartitionLocator();
                    locator.getPartitions()[status.getPid()] = partition;
                }
                
                TabletAsyncClientImpl client = clients.get(endpoint);
                if (IpAddressUtils.isLocalHost(endpoint.getHost())) {
                    logger.info("Find local client with endpoint {} for tid {}, pid {}", endpoint, status.getTid(),
                            status.getPid());
                    partition.setLocalClient(client);
                }
                if (status.getMode().compareTo(Tablet.TableMode.kTableLeader) == 0) {
                    // Leader
                    partition.setWriteClient(client);
                    logger.info("Find write client with endpoint {} for tid {}, pid {}", endpoint, status.getTid(),
                            status.getPid());
                }
                partition.getReadClient().add(client);
            }
        }
        locators = newLocators;
    }
    
    @Override
    public void put(int tid, int pid, String key, long time, byte[] bytes, RpcCallback<PutResponse> done) {
        PartitionLocator locator = getPartionLocator(tid, pid);
        locator.getWriteClient().put(tid, pid, key, time, bytes, done);
    }

    @Override
    public void put(int tid, int pid, String key, long time, String value, RpcCallback<PutResponse> done) {
        PartitionLocator locator = getPartionLocator(tid, pid);
        locator.getWriteClient().put(tid, pid, key, time, value, done);
    }
    
    
    private PartitionLocator getPartionLocator(int tid, int pid) {
        TableLocator locator = locators.get(tid);
        if (locator == null) {
            TabletException e = new TabletException("no client for tid " + tid);
            throw e;
        }
        if (locator.getPartitions().length < pid
                || locator.getPartitions()[pid] == null) {
            TabletException e = new TabletException("no client for pid " + tid);
            throw e;
        }
        return locator.getPartitions()[pid];
    }
    
}

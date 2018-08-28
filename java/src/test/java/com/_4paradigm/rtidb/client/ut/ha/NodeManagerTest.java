package com._4paradigm.rtidb.client.ut.ha;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.ha.NodeManager;

import io.brpc.client.BrpcChannelGroup;
import io.brpc.client.EndPoint;
import io.brpc.client.RpcBaseClient;
public class NodeManagerTest {

    @Test
    public void testSwap() {
        RpcBaseClient bc = new RpcBaseClient();
        NodeManager nm = new NodeManager(bc);
        Assert.assertEquals(0, nm.getEndpointSet().size());
        Set<EndPoint> nodes = new HashSet<EndPoint>();
        nm.update(nodes);
        Assert.assertEquals(0, nm.getEndpointSet().size());
        EndPoint endpoint1 = new EndPoint("127.0.0.1:12345");
        nodes.add(endpoint1);
        nm.update(nodes);
        Assert.assertEquals(1, nm.getEndpointSet().size());
        Assert.assertEquals(endpoint1, nm.getEndpointSet().iterator().next());
        BrpcChannelGroup bcg = nm.getChannel(endpoint1);
        Assert.assertTrue(bcg.getIp().equals("127.0.0.1"));
        Assert.assertEquals(bcg.getPort(), 12345);
        
        nm.update(nodes);
        
        BrpcChannelGroup bcg2 = nm.getChannel(endpoint1);
        // test reuse channel
        Assert.assertEquals(bcg, bcg2);
        
        
        // add a new one
        EndPoint endPoint2 = new EndPoint("127.0.0.1:12346");
        nodes.add(endPoint2);
        
        nm.update(nodes);
        
        BrpcChannelGroup bcg3 = nm.getChannel(endpoint1);
        // test reuse channel
        Assert.assertEquals(bcg, bcg3);
        
        BrpcChannelGroup bcg4 = nm.getChannel(endPoint2);
        Assert.assertTrue(bcg4.getIp().equals("127.0.0.1"));
        Assert.assertEquals(bcg4.getPort(), 12346);
        
        Assert.assertEquals(2, nm.getEndpointSet().size());
        
        Set<EndPoint> newNodes = new HashSet<EndPoint>();
        EndPoint endPoint3 = new EndPoint("127.0.0.1:12347");
        newNodes.add(endPoint3);
        nm.update(newNodes);
        
        Assert.assertEquals(1, nm.getEndpointSet().size());
        BrpcChannelGroup bcg6 = nm.getChannel(endPoint3);
        Assert.assertTrue(bcg6.getIp().equals("127.0.0.1"));
        Assert.assertEquals(bcg6.getPort(), 12347);
        nm.close(); 
    }

}

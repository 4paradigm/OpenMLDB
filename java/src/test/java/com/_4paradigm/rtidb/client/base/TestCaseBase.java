package com._4paradigm.rtidb.client.base;

import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestCaseBase {
    private final static Logger logger = LoggerFactory.getLogger(TestCaseBase.class);
    protected NameServerClientImpl nsc = null ;
    protected RTIDBClusterClient client = null;
    protected TableSyncClient tableSyncClient = null;
    protected TableAsyncClient tableAsyncClient = null;
    protected RTIDBSingleNodeClient snc = null;
    protected TabletClientImpl tabletClient = null;
    protected TabletSyncClientImpl tabletSyncClient = null;
    protected TabletAsyncClientImpl tabletAsyncClient = null;
    protected TableAsyncClient tableSingleNodeAsyncClient = null;
    protected TableSyncClient tableSingleNodeSyncClient = null;

    public void setUp() {
        try {
            nsc = ClientBuilder.buildNewNSC();
            client = ClientBuilder.buildNewCluster();
            tableSyncClient = new TableSyncClientImpl(client);
            tableAsyncClient = new TableAsyncClientImpl(client);
            setUpSingle();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setUpSingle() {
        try {
            snc = ClientBuilder.buildNewSingle();
            tableSingleNodeSyncClient = new TableSyncClientImpl(snc);
            tableSingleNodeAsyncClient = new TableAsyncClientImpl(snc);
            tabletClient = new TabletClientImpl(snc);
            tabletSyncClient = new TabletSyncClientImpl(snc);
            tabletAsyncClient = new TabletAsyncClientImpl(snc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  void tearDown() {
        if (nsc != null)
        nsc.close();
        if (client !=null)
        client.close();
        if (snc != null)
        snc.close();
    }

}

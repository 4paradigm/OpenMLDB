package com._4paradigm.rtidb.client.base;

import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletClientImpl;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;
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

    public void setUp() {
        try {
            nsc = ClientBuilder.buildNewNSC();
            client = ClientBuilder.buildNewCluster();
            snc = ClientBuilder.buildNewSingle();
            tableSyncClient = new TableSyncClientImpl(client);
            tableAsyncClient = new TableAsyncClientImpl(client);
            tabletClient = new TabletClientImpl(snc);
            tabletSyncClient = new TabletSyncClientImpl(snc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  void tearDown() {
        nsc.close();
        client.close();
        snc.close();
    }

}

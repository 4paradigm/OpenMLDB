package com._4paradigm.rtidb.client.ut.ha;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBNSClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;

public class TableNsClientTest {

    @Test
    public void test() throws TimeoutException, TabletException {
        RTIDBClientConfig config = new RTIDBClientConfig();
        config.setNsEndpoint("172.27.128.32:6527");
        RTIDBNSClient client = new RTIDBNSClient(config);
        TableSyncClient table = new TableSyncClientImpl(client);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("sd_key", "card");
        table.put("crd",1111, map);
        
    }
}

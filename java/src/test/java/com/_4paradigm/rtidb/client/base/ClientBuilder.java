package com._4paradigm.rtidb.client.base;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import io.brpc.client.EndPoint;

public class ClientBuilder {
    public static RTIDBClientConfig config = new RTIDBClientConfig();
    static {
        String zkEndpoints = Config.ZK_ENDPOINTS;
        String zkRootPath = Config.ZK_ROOT_PATH;
        config = new RTIDBClientConfig();
        config.setZkEndpoints(zkEndpoints);
        config.setZkRootPath(zkRootPath);
        config.setWriteTimeout(Config.WRITE_TIMEOUT);
        config.setReadTimeout(Config.READ_TIMEOUT);
        config.setGlobalReadStrategies(TableHandler.ReadStrategy.kReadLeader);
    }

    public static RTIDBClusterClient buildNewCluster() {
        try {
            RTIDBClusterClient client = new RTIDBClusterClient(config);
            client.init();
            return client;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public static RTIDBSingleNodeClient buildNewSingle() {
        try {
            EndPoint endpoint = new EndPoint(Config.ENDPOINT);
            RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);
            snc.init();
            return snc;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static NameServerClientImpl buildNewNSC() {
        try {
            NameServerClientImpl nsc = new NameServerClientImpl(config);
            nsc.init();
            return nsc;
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

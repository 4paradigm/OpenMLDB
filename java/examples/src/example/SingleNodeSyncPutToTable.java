package example;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;

import io.brpc.client.EndPoint;

public class SingleNodeSyncPutToTable {

    
    public static void main(String[] args) {
        TableSyncClient tableClient = null;        
        EndPoint endpoint = new EndPoint("172.27.128.32:9527");
        RTIDBClientConfig config = new RTIDBClientConfig();
        RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);
        try {
            snc.init();
            tableClient = new TableSyncClientImpl(snc);
            boolean ok = tableClient.put(5, 0, 9527, new Object[] {"card0", "merchant0", 9.1d});
            if (ok) {
                System.out.println("put ok");
            }else {
                System.out.println("put failed");
            }
            //释放资源
            snc.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}

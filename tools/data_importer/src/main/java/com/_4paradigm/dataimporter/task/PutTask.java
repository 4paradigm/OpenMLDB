package com._4paradigm.dataimporter.task;

import com._4paradigm.rtidb.client.TableSyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

public class PutTask implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(PutTask.class);
    private String id;
    private TableSyncClient tableSyncClient;
    private String tableName ;
    private HashMap map;


    public PutTask(String id, TableSyncClient tableSyncClient, String tableName,HashMap map){
        this.id=id;
        this.tableSyncClient=tableSyncClient;
        this.tableName=tableName;
        this.map=map;
    }
    @Override
    public void run() {
        while (true) {//当put失败时进行重试，直到成功
            try {
                if(map==null){
                    logger.error("error: the map is null");
                    return;
                }
                if (tableSyncClient.put(tableName, System.currentTimeMillis(), map)) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//        try {
//            Thread.sleep(3000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        logger.info(Thread.currentThread().getName() + " -> " + "Current Task Num: #" + id);
    }
    @Override
    public String toString() {
        return "$classname{" + "Task Num=" + id + '}';
    }
}


package com._4paradigm.dataimporter.initialization;

public class InitAll {
    public static void init(){
        InitProperties.initProperties();
        OperateTable.initClient();
        OperateThreadPool.initThreadPool();
    }
}

package com._4paradigm.openmldb.server.impl;

import com._4paradigm.openmldb.server.NLTablet;
import com._4paradigm.openmldb.server.NLTabletServer;

public class NLTabletServerImpl implements NLTabletServer {

    @Override
    public NLTablet.CreateTableResponse createTable(NLTablet.CreateTableRequest request) {
        NLTablet.CreateTableResponse.Builder builder = NLTablet.CreateTableResponse.newBuilder();
        builder.setCode(0).setMsg("ok");
        NLTablet.CreateTableResponse response = builder.build();
        return response;
    }
}

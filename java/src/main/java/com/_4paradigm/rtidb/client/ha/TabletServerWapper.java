package com._4paradigm.rtidb.client.ha;

import rtidb.api.TabletServer;

public class TabletServerWapper {
    private TabletServer server;
    private String endpoint;

    public TabletServerWapper(String endpoint, TabletServer server) {
        this.endpoint = endpoint;
        this.server = server;
    }

    public TabletServer getServer() {
        return server;
    }

    public String getEndpoint() {
        return endpoint;
    }

}

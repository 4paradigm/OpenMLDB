package com._4paradigm.rtidb.client.ha;

import java.util.ArrayList;
import java.util.List;

import rtidb.api.TabletServer;

public class PartitionHandler {

    private TabletServer leader = null;
    private List<TabletServer> followers = new ArrayList<TabletServer>();
    public TabletServer getLeader() {
        return leader;
    }
    public void setLeader(TabletServer leader) {
        this.leader = leader;
    }
    public List<TabletServer> getFollowers() {
        return followers;
    }
    public void setFollowers(List<TabletServer> followers) {
        this.followers = followers;
    }
    
}

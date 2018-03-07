package com._4paradigm.rtidb.client.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com._4paradigm.rtidb.client.ha.TableHandler.ReadStrategy;

import rtidb.api.TabletServer;

public class PartitionHandler {
    private final static Random rand = new Random(System.currentTimeMillis());
    private TabletServer leader = null;
    private List<TabletServer> followers = new ArrayList<TabletServer>();
    // the fast server for tablet read
    private TabletServer fastTablet = null;
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
    public TabletServer getFastTablet() {
        return fastTablet;
    }
    public void setFastTablet(TabletServer fastTablet) {
        this.fastTablet = fastTablet;
    }
    
    public TabletServer getReadHandler(ReadStrategy strategy) {
        // single node tablet
        if (followers.size() <= 0 || strategy == null) {
            return leader;
        }
        switch(strategy) {
        case kReadLeader:
            return leader;
        case kReadLocal:
            if (fastTablet != null) {
                return fastTablet;
            }else if(followers.size() > 0) {
                int index = (int) ((rand.nextFloat() * 1000) % followers.size());
                return followers.get(index);
            }else {
                return leader;
            }
        default:
            return leader;
        }
    }
    
    
}

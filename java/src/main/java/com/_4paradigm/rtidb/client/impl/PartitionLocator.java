package com._4paradigm.rtidb.client.impl;

import java.util.HashSet;
import java.util.Set;

public class PartitionLocator {
    private int pid;
    // the partition for write
    private TabletAsyncClientImpl writeClient = null;
    // the partition for read
    private Set<TabletAsyncClientImpl> readClient = new HashSet<TabletAsyncClientImpl>();
    // the partition on local host
    private TabletAsyncClientImpl localClient = null;

    /**
     * @return the pid
     */
    public int getPid() {
        return pid;
    }

    /**
     * @param pid
     *            the pid to set
     */
    public void setPid(int pid) {
        this.pid = pid;
    }

    /**
     * @return the writeClient
     */
    public TabletAsyncClientImpl getWriteClient() {
        return writeClient;
    }

    /**
     * @param writeClient
     *            the writeClient to set
     */
    public void setWriteClient(TabletAsyncClientImpl writeClient) {
        this.writeClient = writeClient;
    }

    /**
     * @return the readClient
     */
    public Set<TabletAsyncClientImpl> getReadClient() {
        return readClient;
    }

    /**
     * @param readClient
     *            the readClient to set
     */
    public void setReadClient(Set<TabletAsyncClientImpl> readClient) {
        this.readClient = readClient;
    }

    /**
     * @return the localClient
     */
    public TabletAsyncClientImpl getLocalClient() {
        return localClient;
    }

    /**
     * @param localClient
     *            the localClient to set
     */
    public void setLocalClient(TabletAsyncClientImpl localClient) {
        this.localClient = localClient;
    }
}

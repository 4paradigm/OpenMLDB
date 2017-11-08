package com._4paradigm.rtidb.client.ha;

public class TableLocator {

    private int tid;
    private PartitionLocator[]  partitions = new PartitionLocator[64];
    
    /**
     * @return the tid
     */
    public int getTid() {
        return tid;
    }


    /**
     * @param tid the tid to set
     */
    public void setTid(int tid) {
        this.tid = tid;
    }


    /**
     * @return the partitions
     */
    public PartitionLocator[] getPartitions() {
        return partitions;
    }


    /**
     * @param partitions the partitions to set
     */
    public void setPartitions(PartitionLocator[] partitions) {
        this.partitions = partitions;
    }



    

}

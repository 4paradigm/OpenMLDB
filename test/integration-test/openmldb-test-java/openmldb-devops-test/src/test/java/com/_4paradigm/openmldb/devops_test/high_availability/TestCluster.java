package com._4paradigm.openmldb.devops_test.high_availability;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;

public class TestCluster extends ClusterTest {
    public void testMoreReplica(){
        // 创建磁盘表和内存表。

        // 其中一个tablet stop，leader 内存表和磁盘表可以正常访问，flower 内存表和磁盘表可以正常访问。
        // tablet start，数据可以回复，要看磁盘表和内存表。
        //创建磁盘表和内存表，在重启tablet，数据可回复，内存表和磁盘表可以正常访问。
        //创建磁盘表和内存表，插入一些数据，然后make snapshot，在重启tablet，数据可回复。
        //tablet 依次restart，数据可回复，可以访问。
        //3个tablet stop，不能访问。
        // 1个tablet启动，数据可回复，分片所在的表，可以访问。
        //ns stop，可以正常访问。
        //2个ns stop，不能访问。
        //ns start 可以访问。
        //一个 zk stop，可以正常访问
        //3个zk stop，不能正常访问。
        //一个zk start，可正常访问。
        //3个 zk start，可正常访问。
        // 一个节点（ns leader 所在服务器）重启，leader可以正常访问，flower可以正常访问。
        //一直查询某一个表，然后重启一个机器。
    }
}

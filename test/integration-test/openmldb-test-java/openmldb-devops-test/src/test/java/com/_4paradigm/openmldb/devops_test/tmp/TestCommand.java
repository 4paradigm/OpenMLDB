package com._4paradigm.openmldb.devops_test.tmp;

import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import org.testng.annotations.Test;

import java.util.List;

public class TestCommand {
    @Test
    public void test1(){
        List<String> list = ExecutorUtil.run("/Users/zhaowei/openmldb-auto-test/tmp_mac/openmldb-ns-1/bin/openmldb --zk_cluster=127.0.0.1:30000 --zk_root_path=/openmldb --role=ns_client --interactive=false --database=test_devops4 --cmd='showopstatus'");
        list.forEach(System.out::println);
    }
    @Test
    public void test2(){
        List<String> list = ExecutorUtil.run("/home/zhaowei01/openmldb-auto-test/tmp/openmldb-ns-1/bin/openmldb --zk_cluster=172.24.4.55:30000 --zk_root_path=/openmldb --role=ns_client --interactive=false --database=test_devops4 --cmd='showopstatus'");
        list.forEach(System.out::println);
    }
}

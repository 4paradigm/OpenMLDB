package com._4paradigm.openmldb.devops_test.tmp;

import com._4paradigm.openmldb.devops_test.common.ClusterTest;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFacade;
import com._4paradigm.openmldb.test_common.command.chain.SqlChainManager;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.test_tool.command_tool.common.CommandUtil;
import com._4paradigm.test_tool.command_tool.common.ExecUtil;
import com._4paradigm.test_tool.command_tool.common.ExecutorUtil;
import org.testng.annotations.Test;

import java.util.List;

public class TestCommand extends ClusterTest {
    @Test
    public void test1(){
        List<String> list = ExecutorUtil.run("/Users/zhaowei/openmldb-auto-test/tmp_mac/openmldb-ns-1/bin/openmldb --zk_cluster=127.0.0.1:30000 --zk_root_path=/openmldb --role=ns_client --interactive=false --database=test_devops4 --cmd='showopstatus'");
        list.forEach(System.out::println);
    }
    //
    @Test
    public void test3(){
        List<String> list = ExecutorUtil.run("/Users/zhaowei/openmldb-auto-test/tmp_mac/openmldb-ns-1/bin/openmldb --zk_cluster=127.0.0.1:30000 --zk_root_path=/openmldb --role=sql_client --interactive=false --database=test_devops --cmd='select * from test_ssd;'");
        System.out.println("---");
        list.forEach(System.out::println);
    }
    @Test
    public void test4(){
        String str = ExecUtil.exeCommand("/Users/zhaowei/openmldb-auto-test/tmp_mac/openmldb-ns-1/bin/openmldb --zk_cluster=127.0.0.1:30000 --zk_root_path=/openmldb --role=ns_client --interactive=false --database=test_devops4 --cmd='showopstatus'");
        System.out.println("str = " + str);
    }
    @Test
    public void test2(){
        List<String> list = ExecutorUtil.run("/home/zhaowei01/openmldb-auto-test/tmp/openmldb-ns-1/bin/openmldb --zk_cluster=172.24.4.55:30000 --zk_root_path=/openmldb --role=ns_client --interactive=false --database=test_devops4 --cmd='showopstatus'");
        list.forEach(System.out::println);
    }
    @Test
    public void test5(){
        String str = ExecUtil.exeCommand("/Users/zhaowei/openmldb-auto-test/tmp_mac/openmldb-ns-1/bin/openmldb --zk_cluster=127.0.0.1:30000 --zk_root_path=/openmldb --role=sql_client --interactive=false --database=test_devops --cmd='select * from test_ssd;'");
        System.out.println("str = " + str);
    }
    @Test
    public void test6(){
        OpenMLDBResult openMLDBResult = OpenMLDBCommandFacade.sql(OpenMLDBGlobalVar.mainInfo, "test1", "show table status;");
        List<List<Object>> result = openMLDBResult.getResult();
        result.forEach(l->System.out.println(l));
    }
    @Test
    public void test7(){
        OpenMLDBResult openMLDBResult = OpenMLDBCommandFacade.sql(OpenMLDBGlobalVar.mainInfo, "test1", "show databases;");
        List<List<Object>> result = openMLDBResult.getResult();
        result.forEach(l->System.out.println(l));
    }
}

package com._4paradigm.fesql_auto_test.entity;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.testng.annotations.DataProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/7 12:10 PM
 */
@Data
@Builder
public class FEDBInfo {
    private String basePath;
    private String fedbPath;
    private String zk_cluster;
    private String zk_root_path;
    private int nsNum;
    private List<String> nsEndpoints = new ArrayList<>();
    private List<String> nsNames = new ArrayList<>();
    private int tabletNum;
    private List<String> tabletEndpoints = new ArrayList<>();
    private List<String> tabletNames = new ArrayList<>();
    private int blobServerNum;
    private List<String> blobServerEndpoints = new ArrayList<>();
    private List<String> tblobServerNames = new ArrayList<>();
    private int blobProxyNum;
    private List<String> blobProxyEndpoints = new ArrayList<>();
    private List<String> blobProxyNames = new ArrayList<>();
    private String runCommand;

    public String getRunCommand(){
        String runCommand = fedbPath+" --zk_cluster="+zk_cluster+" --zk_root_path="+zk_root_path+" --role=sql_client";
        return runCommand;
    }
}

package com._4paradigm.dataimporter.example;

import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.ns.NS;

import java.util.HashMap;

public class TestPerformance {
    private static String tableName_yes = "performanceTest_yes";
//    private static String tableName_no = "performanceTest_no";

    private static String zkEndpoints = "172.27.128.37:7181,172.27.128.37:7182,172.27.128.37:7183";  // 配置zk地址, 和集群启动配置中的zk_cluster保持一致
    private static String zkRootPath = "/rtidb_cluster";   // 配置集群的zk根路径, 和集群启动配置中的zk_root_path保持一致
    // 下面这几行变量定义不需要改
    private static String leaderPath = zkRootPath + "/leader";
    // NameServerClientImpl要么做成单例, 要么用完之后就调用close, 否则会导致fd泄露
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient clusterClient = null;
    // 发送同步请求的client
    private static TableSyncClient tableSyncClient = null;
    // 发送异步请求的client
    private static TableAsyncClient tableAsyncClient = null;

    static {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkRootPath(zkRootPath);
            // 设置读策略. 默认是读leader
            // 可以按照表级别来设置
            // Map<String, ReadStrategy> strategy = new HashMap<String, ReadStrategy>();
            // strategy put传入的参数是表名和读策略. 读策略可以设置为读leader(kReadLeader)或者读本地(kReadLocal).
            // 读本地的策略是客户端优先选择读取部署在当前机器的节点, 如果当前机器没有部署tablet则随机选择一个从节点读取, 如果没有从节点就读主
            // strategy.put("test1", ReadStrategy.kReadLocal);
            // config.setReadStrategies(strategy);
            // 如果要过滤掉同一个pk下面相同ts的值添加如下设置
            // config.setRemoveDuplicateByTime(true);
            // 设置最大重试次数
            // config.setMaxRetryCnt(3);
            clusterClient = new RTIDBClusterClient(config);
            clusterClient.init();
            tableSyncClient = new TableSyncClientImpl(clusterClient);
            tableAsyncClient = new TableAsyncClientImpl(clusterClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createSchemaTable() {
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder = NS.TableInfo.newBuilder()
                .setName(tableName_yes)  // 设置表名
                .setReplicaNum(1)    // 设置副本数. 此设置是可选的, 默认为3
                //.setPartitionNum(8)  // 设置分片数. 此设置是可选的, 默认为16
                .setCompressType(NS.CompressType.kSnappy) // 设置数据压缩类型. 此设置是可选的默认为不压缩
                //.setTtlType("kLatestTime")  // 设置ttl类型. 此设置是可选的, 默认为"kAbsoluteTime"按时间过期
                .setTtl(0);      // 设置ttl. 如果ttl类型是kAbsoluteTime, 那么ttl的单位是分钟.
        // 设置schema信息
        NS.ColumnDesc col0 = NS.ColumnDesc.newBuilder()
                .setName("PAN")    // 设置字段名
                .setAddTsIdx(true)  // 设置是否为index, 如果设置为true表示该字段为维度列, 查询的时候可以通过此列来查询, 否则设置为false
                .setType("string")  // 设置字段类型, 支持的字段类型有[int32, uint32, int64, uint64, float, double, string]
                .build();
        NS.ColumnDesc col1 = NS.ColumnDesc.newBuilder().setName("HDR-FLD").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col2 = NS.ColumnDesc.newBuilder().setName("FIID").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col3 = NS.ColumnDesc.newBuilder().setName("RES-1").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col4 = NS.ColumnDesc.newBuilder().setName("PROD-IND").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col5 = NS.ColumnDesc.newBuilder().setName("TRAN-DAT").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col6 = NS.ColumnDesc.newBuilder().setName("TRAN-TIM").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col7 = NS.ColumnDesc.newBuilder().setName("PREFIX-LGTH").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col8 = NS.ColumnDesc.newBuilder().setName("TRAN-CDE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col9 = NS.ColumnDesc.newBuilder().setName("TRAN-AMT1").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col10 = NS.ColumnDesc.newBuilder().setName("TRAN-AMT2").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col11 = NS.ColumnDesc.newBuilder().setName("TRAN-AMT3").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col12 = NS.ColumnDesc.newBuilder().setName("MSG-TYP").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col13 = NS.ColumnDesc.newBuilder().setName("APPRV-CDE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col14 = NS.ColumnDesc.newBuilder().setName("RESP-CDE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col15 = NS.ColumnDesc.newBuilder().setName("CRD-VRFY-FLG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col16 = NS.ColumnDesc.newBuilder().setName("CVD-PRESENT-FLG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col17 = NS.ColumnDesc.newBuilder().setName("ORIG-CRNCY-CDE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col18 = NS.ColumnDesc.newBuilder().setName("OACQ-CODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col19 = NS.ColumnDesc.newBuilder().setName("FOR-CODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col20 = NS.ColumnDesc.newBuilder().setName("CRD-ACCPT-ID-NUM").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col21 = NS.ColumnDesc.newBuilder().setName("PT-SRV-COND-CDE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col22 = NS.ColumnDesc.newBuilder().setName("PT-SRV-ENTRY-MDE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col23 = NS.ColumnDesc.newBuilder().setName("PIN-IND").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col24 = NS.ColumnDesc.newBuilder().setName("PIN-CHK").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col25 = NS.ColumnDesc.newBuilder().setName("EC-FLG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col26 = NS.ColumnDesc.newBuilder().setName("TRACK-IND").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col27 = NS.ColumnDesc.newBuilder().setName("ACCT-TYP").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col28 = NS.ColumnDesc.newBuilder().setName("ACCT-STAT").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col29 = NS.ColumnDesc.newBuilder().setName("OPN-DAT").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col30 = NS.ColumnDesc.newBuilder().setName("ACCTCREDLIMIT").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col31 = NS.ColumnDesc.newBuilder().setName("ACCT-AUTH").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col32 = NS.ColumnDesc.newBuilder().setName("LASTPYDAY").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col33 = NS.ColumnDesc.newBuilder().setName("LASTPYAMT").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col34 = NS.ColumnDesc.newBuilder().setName("ACCTCURRBAL").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col35 = NS.ColumnDesc.newBuilder().setName("TERMID").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col36 = NS.ColumnDesc.newBuilder().setName("SHOPNAME").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col37 = NS.ColumnDesc.newBuilder().setName("TRANACQCNTRY").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col38 = NS.ColumnDesc.newBuilder().setName("CUSTRNAME").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col39 = NS.ColumnDesc.newBuilder().setName("CUSTRNBR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col40 = NS.ColumnDesc.newBuilder().setName("CUSTRDOB").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col41 = NS.ColumnDesc.newBuilder().setName("CUSTRMTHRNAME").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col42 = NS.ColumnDesc.newBuilder().setName("CUSTRHOMEPH").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col43 = NS.ColumnDesc.newBuilder().setName("CUSTRWORKPH").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col44 = NS.ColumnDesc.newBuilder().setName("CUSTRMOBILE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col45 = NS.ColumnDesc.newBuilder().setName("CUSTREMAIL").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col46 = NS.ColumnDesc.newBuilder().setName("CARDPRODUCT").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col47 = NS.ColumnDesc.newBuilder().setName("CARDISSDAY").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col48 = NS.ColumnDesc.newBuilder().setName("ACCTADDRDAY").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col49 = NS.ColumnDesc.newBuilder().setName("CARDSTCHGDAY").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col50 = NS.ColumnDesc.newBuilder().setName("CARDSTATCODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col51 = NS.ColumnDesc.newBuilder().setName("ACCTHICASH").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col52 = NS.ColumnDesc.newBuilder().setName("ACCTHIPUR").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col53 = NS.ColumnDesc.newBuilder().setName("LASTAUTHDY").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col54 = NS.ColumnDesc.newBuilder().setName("LASTAUTHDYX").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col55 = NS.ColumnDesc.newBuilder().setName("TRANSSRC").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col56 = NS.ColumnDesc.newBuilder().setName("TRANNOTIFFLAG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col57 = NS.ColumnDesc.newBuilder().setName("NOTIFICATION-RSN").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col58 = NS.ColumnDesc.newBuilder().setName("CARDHOLDER").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col59 = NS.ColumnDesc.newBuilder().setName("TRANMCC").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col60 = NS.ColumnDesc.newBuilder().setName("ACCTAVLPUR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col61 = NS.ColumnDesc.newBuilder().setName("ACCTAVLCASH").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col62 = NS.ColumnDesc.newBuilder().setName("ACCXCURRBAL").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col63 = NS.ColumnDesc.newBuilder().setName("ACCXAVLPUR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col64 = NS.ColumnDesc.newBuilder().setName("ACCXAVLCASH").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col65 = NS.ColumnDesc.newBuilder().setName("ACCXAUTHAMT").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col66 = NS.ColumnDesc.newBuilder().setName("LASTPAYDAYX").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col67 = NS.ColumnDesc.newBuilder().setName("ACCXLASTPYAMT").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col68 = NS.ColumnDesc.newBuilder().setName("ACCXHICASH").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col69 = NS.ColumnDesc.newBuilder().setName("ACCXHIPUR").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col70 = NS.ColumnDesc.newBuilder().setName("CUSTRUPDDAY").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col71 = NS.ColumnDesc.newBuilder().setName("ECI-FLAG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col72 = NS.ColumnDesc.newBuilder().setName("UCAF-FLAG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col73 = NS.ColumnDesc.newBuilder().setName("Link1Name ").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col74 = NS.ColumnDesc.newBuilder().setName("Link1Ph").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col75 = NS.ColumnDesc.newBuilder().setName("Link1Mp").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col76 = NS.ColumnDesc.newBuilder().setName("Link2Name").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col77 = NS.ColumnDesc.newBuilder().setName("Link2Ph").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col78 = NS.ColumnDesc.newBuilder().setName("Link2Mp").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col79 = NS.ColumnDesc.newBuilder().setName("TRANDATETIME").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col80 = NS.ColumnDesc.newBuilder().setName("TRANTRACENBR").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col81 = NS.ColumnDesc.newBuilder().setName("PROCCODE").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col82 = NS.ColumnDesc.newBuilder().setName("TRANTYPE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col83 = NS.ColumnDesc.newBuilder().setName("TRANRSPOUT").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col84 = NS.ColumnDesc.newBuilder().setName("CARDACTDAY").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col85 = NS.ColumnDesc.newBuilder().setName("ACCTCYCLENBR").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col86 = NS.ColumnDesc.newBuilder().setName("CARDPRODLVL").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col87 = NS.ColumnDesc.newBuilder().setName("TRANMOTOFLAG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col88 = NS.ColumnDesc.newBuilder().setName("TRANSIGNACCU").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col89 = NS.ColumnDesc.newBuilder().setName("CUSTRUPDDT").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col90 = NS.ColumnDesc.newBuilder().setName("CARDPINCHK").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col91 = NS.ColumnDesc.newBuilder().setName("ACCTNBR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col92 = NS.ColumnDesc.newBuilder().setName("CUSTRGENDER").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col93 = NS.ColumnDesc.newBuilder().setName("COMP-NAME").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col94 = NS.ColumnDesc.newBuilder().setName("MP-LIMT").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col95 = NS.ColumnDesc.newBuilder().setName("MP-LIMT-L").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col96 = NS.ColumnDesc.newBuilder().setName("MP-FLAG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col97 = NS.ColumnDesc.newBuilder().setName("MP-AV").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col98 = NS.ColumnDesc.newBuilder().setName("SMS-YN").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col99 = NS.ColumnDesc.newBuilder().setName("RUSH-FLAG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col100 = NS.ColumnDesc.newBuilder().setName("ISSNBR").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col101 = NS.ColumnDesc.newBuilder().setName("CLASSCODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col102 = NS.ColumnDesc.newBuilder().setName("CITYCODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col103 = NS.ColumnDesc.newBuilder().setName("PIN-FLAG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col104 = NS.ColumnDesc.newBuilder().setName("IC-FLAG").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col105 = NS.ColumnDesc.newBuilder().setName("AC-PAYMT").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col106 = NS.ColumnDesc.newBuilder().setName("POS-CODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col107 = NS.ColumnDesc.newBuilder().setName("ACTION-CODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col108 = NS.ColumnDesc.newBuilder().setName("NATION-CD").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col109 = NS.ColumnDesc.newBuilder().setName("ID-ISSDT").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col110 = NS.ColumnDesc.newBuilder().setName("ID-DTE").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col111 = NS.ColumnDesc.newBuilder().setName("LAYERCODE2").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col112 = NS.ColumnDesc.newBuilder().setName("VALID-FROM").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col113 = NS.ColumnDesc.newBuilder().setName("EXPIRY-DTE").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col114 = NS.ColumnDesc.newBuilder().setName("PIN-FAILS").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col115 = NS.ColumnDesc.newBuilder().setName("CVN-FAILS").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col116 = NS.ColumnDesc.newBuilder().setName("PIN-LIM").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col117 = NS.ColumnDesc.newBuilder().setName("POINT").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col118 = NS.ColumnDesc.newBuilder().setName("SETTL-CURRCYCD").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col119 = NS.ColumnDesc.newBuilder().setName("TXNINITATE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col120 = NS.ColumnDesc.newBuilder().setName("FALLBACK").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col121 = NS.ColumnDesc.newBuilder().setName("ISS-MOD").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col122 = NS.ColumnDesc.newBuilder().setName("CU-RISK").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col123 = NS.ColumnDesc.newBuilder().setName("DDA-IND").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col124 = NS.ColumnDesc.newBuilder().setName("LOAN-FLG").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col125 = NS.ColumnDesc.newBuilder().setName("CREDLMT-ADJ").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col126 = NS.ColumnDesc.newBuilder().setName("TOKEN").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col127 = NS.ColumnDesc.newBuilder().setName("AUTH-ATC").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col128 = NS.ColumnDesc.newBuilder().setName("CARD-ATC").setAddTsIdx(false).setType("int32").build();
        NS.ColumnDesc col129 = NS.ColumnDesc.newBuilder().setName("CRED-RSN").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col130 = NS.ColumnDesc.newBuilder().setName("CC-YN").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col131 = NS.ColumnDesc.newBuilder().setName("T3RD-PARTY").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col132 = NS.ColumnDesc.newBuilder().setName("ACCAMT").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col133 = NS.ColumnDesc.newBuilder().setName("CRED-CHDATE").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col134 = NS.ColumnDesc.newBuilder().setName("BRANCH").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col135 = NS.ColumnDesc.newBuilder().setName("SRC-ACC").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col136 = NS.ColumnDesc.newBuilder().setName("CVR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col137 = NS.ColumnDesc.newBuilder().setName("TVR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col138 = NS.ColumnDesc.newBuilder().setName("CVMR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col139 = NS.ColumnDesc.newBuilder().setName("TKSRC").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col140 = NS.ColumnDesc.newBuilder().setName("TKTRID").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col141 = NS.ColumnDesc.newBuilder().setName("INPUT-EXPIRY-DTE").setAddTsIdx(false).setType("int16").build();
        NS.ColumnDesc col142 = NS.ColumnDesc.newBuilder().setName("ON-BEHALF").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col143 = NS.ColumnDesc.newBuilder().setName("FRAUD-SCORING").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col144 = NS.ColumnDesc.newBuilder().setName("SECURITY-ADD").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col145 = NS.ColumnDesc.newBuilder().setName("ADVICE-RC").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col146 = NS.ColumnDesc.newBuilder().setName("AUTH-AGENT-ID").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col147 = NS.ColumnDesc.newBuilder().setName("CARD-NBR").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col148 = NS.ColumnDesc.newBuilder().setName("SEID").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col149 = NS.ColumnDesc.newBuilder().setName("TERMENTRY").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col150 = NS.ColumnDesc.newBuilder().setName("TRANSRC").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col151 = NS.ColumnDesc.newBuilder().setName("PBOC-YN").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col152 = NS.ColumnDesc.newBuilder().setName("ECI").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col153 = NS.ColumnDesc.newBuilder().setName("CVC-RESLT").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col154 = NS.ColumnDesc.newBuilder().setName("CARDVCN_STS").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col155 = NS.ColumnDesc.newBuilder().setName("SOUAREA").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col156 = NS.ColumnDesc.newBuilder().setName("CLASS_CODE").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col157 = NS.ColumnDesc.newBuilder().setName("RESV").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col158 = NS.ColumnDesc.newBuilder().setName("TAILFLD").setAddTsIdx(false).setType("string").build();
        // 将schema添加到builder中
        builder.addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2).addColumnDesc(col3).addColumnDesc(col4).addColumnDesc(col5).addColumnDesc(col6)
                .addColumnDesc(col7).addColumnDesc(col8).addColumnDesc(col9).addColumnDesc(col10).addColumnDesc(col11).addColumnDesc(col12).addColumnDesc(col13)
                .addColumnDesc(col14).addColumnDesc(col15).addColumnDesc(col16).addColumnDesc(col17).addColumnDesc(col18).addColumnDesc(col19).addColumnDesc(col20)
                .addColumnDesc(col21).addColumnDesc(col22).addColumnDesc(col23).addColumnDesc(col24).addColumnDesc(col25).addColumnDesc(col26).addColumnDesc(col27)
                .addColumnDesc(col28).addColumnDesc(col29).addColumnDesc(col30).addColumnDesc(col31).addColumnDesc(col32).addColumnDesc(col33).addColumnDesc(col34)
                .addColumnDesc(col35).addColumnDesc(col36).addColumnDesc(col37).addColumnDesc(col38).addColumnDesc(col39).addColumnDesc(col40).addColumnDesc(col41)
                .addColumnDesc(col42).addColumnDesc(col43).addColumnDesc(col44).addColumnDesc(col45).addColumnDesc(col46).addColumnDesc(col47).addColumnDesc(col48)
                .addColumnDesc(col49).addColumnDesc(col50).addColumnDesc(col51).addColumnDesc(col52).addColumnDesc(col53).addColumnDesc(col54).addColumnDesc(col55)
                .addColumnDesc(col56).addColumnDesc(col57).addColumnDesc(col58).addColumnDesc(col59).addColumnDesc(col60).addColumnDesc(col61).addColumnDesc(col62)
                .addColumnDesc(col63).addColumnDesc(col64).addColumnDesc(col65).addColumnDesc(col66).addColumnDesc(col67).addColumnDesc(col68).addColumnDesc(col69)
                .addColumnDesc(col70).addColumnDesc(col71).addColumnDesc(col72).addColumnDesc(col73).addColumnDesc(col74).addColumnDesc(col75).addColumnDesc(col76)
                .addColumnDesc(col77).addColumnDesc(col78).addColumnDesc(col79).addColumnDesc(col80).addColumnDesc(col81).addColumnDesc(col82).addColumnDesc(col83)
                .addColumnDesc(col84).addColumnDesc(col85).addColumnDesc(col86).addColumnDesc(col87).addColumnDesc(col88).addColumnDesc(col89).addColumnDesc(col90)
                .addColumnDesc(col91).addColumnDesc(col92).addColumnDesc(col93).addColumnDesc(col94).addColumnDesc(col95).addColumnDesc(col96).addColumnDesc(col97)
                .addColumnDesc(col98).addColumnDesc(col99).addColumnDesc(col100).addColumnDesc(col101).addColumnDesc(col102).addColumnDesc(col103).addColumnDesc(col104)
                .addColumnDesc(col105).addColumnDesc(col106).addColumnDesc(col107).addColumnDesc(col108).addColumnDesc(col109).addColumnDesc(col110).addColumnDesc(col111)
                .addColumnDesc(col112).addColumnDesc(col113).addColumnDesc(col114).addColumnDesc(col115).addColumnDesc(col116).addColumnDesc(col117).addColumnDesc(col118)
                .addColumnDesc(col119).addColumnDesc(col120).addColumnDesc(col121).addColumnDesc(col122).addColumnDesc(col123).addColumnDesc(col124).addColumnDesc(col125)
                .addColumnDesc(col126).addColumnDesc(col127).addColumnDesc(col128).addColumnDesc(col129).addColumnDesc(col130).addColumnDesc(col131).addColumnDesc(col132)
                .addColumnDesc(col133).addColumnDesc(col134).addColumnDesc(col135).addColumnDesc(col136).addColumnDesc(col137).addColumnDesc(col138).addColumnDesc(col139)
                .addColumnDesc(col140).addColumnDesc(col141).addColumnDesc(col142).addColumnDesc(col143).addColumnDesc(col144).addColumnDesc(col145).addColumnDesc(col146)
                .addColumnDesc(col147).addColumnDesc(col148).addColumnDesc(col149).addColumnDesc(col150).addColumnDesc(col151).addColumnDesc(col152).addColumnDesc(col153)
                .addColumnDesc(col154).addColumnDesc(col155).addColumnDesc(col156).addColumnDesc(col157).addColumnDesc(col158);
        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        System.out.println("the result that the schema is created ：" + ok);
        clusterClient.refreshRouteTable();
    }

    public static void putSchemaTable(){
//        long ts = System.currentTimeMillis();
        HashMap<String,Object> map=new HashMap<>();
        PutFuture pf = null;
        for(int i=0;i<100000000;i++) {
            map.put("PAN", String.valueOf(1000000000000000l+i));
            map.put("HDR-FLD", "HEAD50");
            map.put("FIID", "12345678910");
            map.put("RES-1", "00000000000000");
            map.put("PROD-IND", "06");
            map.put("TRAN-DAT", 11223344);
            map.put("TRAN-TIM", 11223344);
            map.put("PREFIX-LGTH", (short) 11);
            map.put("TRAN-CDE", "12345");
            map.put("TRAN-AMT1", "012345678910");

            map.put("TRAN-AMT2", "012345678910");
            map.put("TRAN-AMT3", "012345678910");
            map.put("MSG-TYP", "0123");
            map.put("APPRV-CDE", "012345");
            map.put("RESP-CDE", "012345");
            map.put("CRD-VRFY-FLG", "0");
            map.put("CVD-PRESENT-FLG", "0");
            map.put("ORIG-CRNCY-CDE", "111");
            map.put("ACQ-CODE", "01234567890");
            map.put("AFOR-CODE", "01234567890");

            map.put("ACRD-ACCPT-ID-NUM", "012345678901234");
            map.put("PT-SRV-COND-CDE", "34");
            map.put("PT-SRV-ENTRY-MDE", "1234");
            map.put("PIN-IND", "1");
            map.put("PPIN-CHK", "1");
            map.put("EC-FLG", "1");
            map.put("TRACK-IND", "1");
            map.put("ACCT-TYP", (short) 12);
            map.put("ACCT-STAT", "12");
            map.put("OPN-DAT", 12345678);

            map.put("ACCTCREDLIMIT", 123456789012l);
            map.put("ACCT-AUTH", 123456789012l);
            map.put("LASTPYDAY", 12345678);
            map.put("LASTPYAMT", 123456789012l);
            map.put("ACCTCURRBAL", "123456789012");
            map.put("TERMID", "12345678");
            map.put("SHOPNAME", "1234567890123456789012345678901234567890");
            map.put("TRANACQCNTRY", "123");
            map.put("CUSTRNAME", "1234567890123456789012345678901234567890");
            map.put("CUSTRNBR", "12345678901234567890");

            map.put("CUSTRDOB", 12345678);
            map.put("CUSTRMTHRNAME", "1234567890123456789012345678901234567890");
            map.put("CUSTRHOMEPH", "12345678901234567890");
            map.put("CUSTRWORKPH", "12345678901234567890");
            map.put("CUSTRMOBILE", "12345678901234567890");
            map.put("CUSTREMAIL", "1234567890123456789012345678901234567890");
            map.put("CARDPRODUCT", "12345678");
            map.put("CARDISSDAY", 12345678);
            map.put("ACCTADDRDAY", 12345678);
            map.put("CARDSTCHGDAY", 12345678);

            map.put("CARDSTATCODE", "1234");
            map.put("ACCTHICASH", 123456789012l);
            map.put("ACCTHIPUR", 123456789012l);
            map.put("LASTAUTHDY", 12345678);
            map.put("LASTAUTHDYX", 12345678);
            map.put("TRANSSRC", "1");
            map.put("TRANNOTIFFLAG", "1");
            map.put("NOTIFICATION-RSN", "12");
            map.put("CARDHOLDER", "1");
            map.put("TRANMCC", "1234");

            map.put("ACCTAVLPUR", "123456789012");
            map.put("ACCTAVLCASH", "123456789012");
            map.put("ACCXCURRBAL", "123456789012");
            map.put("ACCXAVLPUR", "123456789012");
            map.put("ACCXAVLCASH", "123456789012");
            map.put("ACCXAUTHAMT", "123456789012");
            map.put("LASTPAYDAYX", 12345678);
            map.put("ACCXLASTPYAMT", 123456789012l);
            map.put("ACCXHICASH", 123456789012l);
            map.put("ACCXHIPUR", 123456789012l);

            map.put("CUSTRUPDDAY", 12345678);
            map.put("ECI-FLAG", "1");
            map.put("UCAF-FLAG", "1");
            map.put("Link1Name", "123456789012345678901234567890");
            map.put("Link1Ph", "12345678901234567");
            map.put("Link1Mp", "123456789012");
            map.put("Link2Name", "123456789012345678901234567890");
            map.put("Link2Ph", "12345678901234567");
            map.put("Link2Mp", "123456789012");
            map.put("TRANDATETIME", 1234567890l);

            map.put("TRANTRACENBR", 123456);
            map.put("PROCCODE", 123456);
            map.put("TRANTYPE", "12");
            map.put("TRANRSPOUT", "12");
            map.put("CARDACTDAY", 12345678);
            map.put("ACCTCYCLENBR", (short) 12);
            map.put("CARDPRODLVL", (short) 12);
            map.put("TRANMOTOFLAG", "1");
            map.put("TRANSIGNACCU", "12");
            map.put("CUSTRUPDDT", "12345678901234");

            map.put("CARDPINCHK", "1");
            map.put("ACCTNBR", "1234567890");
            map.put("CUSTRGENDER", "1");
            map.put("COMP-NAME", "123456789012345678901234567890");
            map.put("MP-LIMT", 123456789012l);
            map.put("MP-LIMT-L", 123456789012l);
            map.put("MP-FLAG", "1");
            map.put("MP-AV", 123456789012l);
            map.put("SMS-YN", (short) 1);
            map.put("RUSH-FLAG", "1");

            map.put("ISSNBR", (short) 1);
            map.put("CLASSCODE", "123");
            map.put("CITYCODE", "1234");
            map.put("PIN-FLAG", "1");
            map.put("IC-FLAG", (short) 1);
            map.put("AC-PAYMT", 123456789012l);
            map.put("POS-CODE", "123456789012");
            map.put("ACTION-CODE", "123");
            map.put("NATION-CD", "123");
            map.put("ID-ISSDT", 12345678);

            map.put("ID-DTE", 12345678);
            map.put("LAYERCODE2", "12345");
            map.put("VALID-FROM", (short) 1234);
            map.put("EXPIRY-DTE", (short) 1234);
            map.put("PIN-FAILS", (short) 12);
            map.put("CVN-FAILS", (short) 12);
            map.put("PIN-LIM", 123456789012l);
            map.put("POINT", 1234567890l);
            map.put("SETTL-CURRCYCD", "123");
            map.put("TXNINITATE", "1");

            map.put("FALLBACK", "1");
            map.put("ISS-MOD", "12");
            map.put("CU-RISK", "12");
            map.put("DDA-IND", "1");
            map.put("LOAN-FLG", "1");
            map.put("CREDLMT-ADJ", 1234567890l);
            map.put("TOKEN", "1234567890123456789");
            map.put("AUTH-ATC", 123456);
            map.put("CARD-ATC", 123456);
            map.put("CRED-RSN", "12");

            map.put("CC-YN", (short) 1);
            map.put("T3RD-PARTY", "1");
            map.put("ACCAMT", 123456789l);
            map.put("CRED-CHDATE", 12345678l);
            map.put("BRANCH", (short) 1234);
            map.put("SRC-ACC", "12345678901234567890123456789012");
            map.put("CVR", "123456789012");
            map.put("TVR", "1234567890");
            map.put("CVMR", "123456");
            map.put("TKSRC", (short) 12);

            map.put("TKTRID", "12345678901");
            map.put("INPUT-EXPIRY-DTE", (short) 1234);
            map.put("ON-BEHALF", "123");
            map.put("FRAUD-SCORING", "123456789012");
            map.put("SECURITY-ADD", "123456");
            map.put("ADVICE-RC", "123");
            map.put("AUTH-AGENT-ID", "123456");
            map.put("CARD-NBR", "1234567890123456789");
            map.put("SEID", "123456789012345678901234567890123456789012345678");
            map.put("TERMENTRY", "1");

            map.put("TRANSRC", "1");
            map.put("PBOC-YN", "1");
            map.put("ECI", "123");
            map.put("CVC-RESLT", "1");
            map.put("CARDVCN_STS", "1");
            map.put("SOUAREA", "1");
            map.put("CLASS_CODE", "123");
            map.put("RESV", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"); //+
            //"1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
            //"1234567890123456789012345678901234567890123456789012345678901234567");
            map.put("TAILFLD", "1234");

            try {
                pf = tableAsyncClient.put(tableName_yes, System.currentTimeMillis(), map);
                System.out.println("结果：" + pf);
                map.clear();
                if (pf.get() == false) {
                    System.out.println(" method tableAsyncClient put() failed");
                    throw new Exception("method tableAsyncClient put() failed");
                }
            } catch (TabletException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
    public static void main(String[] args) {
       createSchemaTable();
//        putSchemaTable();
//        System.out.println("1000"+1);
//        Integer i=new Integer(1);
//        System.out.println((Short)i);
//        int i=1;
//        System.out.println((short)i);
    }
}

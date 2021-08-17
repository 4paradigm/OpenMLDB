package com._4paradigm.openmldb.zk;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ZKConfig {
    private String cluster;
    private String namespace;
    @Builder.Default
    private int sessionTimeout = 5000;
    @Builder.Default
    private int connectionTimeout = 5000;
    @Builder.Default
    private int maxRetries = 10;
    @Builder.Default
    private int baseSleepTime = 1000;

}

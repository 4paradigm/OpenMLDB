package com._4paradigm.rtidb.client.ha;

import java.util.HashMap;
import java.util.Map;

import com._4paradigm.rtidb.client.ha.TableHandler.ReadStrategy;

/**
 * @author wangtaize
 *
 */
public class RTIDBClientConfig {

    public boolean isHandleNull() {
        return handleNull;
    }

    public void setHandleNull(boolean handleNull) {
        this.handleNull = handleNull;
    }

    public enum Mode {
        kSingleNodeMode, kClusterMode
    }
    public static final String NULL_STRING = "!N@U#L$L%";
    public static final String EMPTY_STRING = "!@#$%";

	private boolean enableMetrics = true;
	private String zkEndpoints = "";
	private String zkRootPath = "";
	private String zkTableRootPath = "";
	private String zkTableNotifyPath = "";
	private String zkNodeRootPath = "";
	private int zkSesstionTimeout = 10000;
	private int ioThreadNum = 2;
	private int writeTimeout = 10000;
	private int readTimeout = 10000;
	private int maxCntCnnPerHost = 2;
	private boolean removeDuplicateByTime = false;
    private boolean handleNull = false;
	private int maxRetryCnt = 1;
	private Mode mode;
	private Map<String, ReadStrategy> readStrategies = new HashMap<String, ReadStrategy>();
	private String nsEndpoint;
	private int traverseLimit = 200;
	private int timerBucketSize = 16;


    /**
     * @return the timerBucketSize
     */
    public int getTimerBucketSize() {
        return timerBucketSize;
    }

    /**
     * @param timerBucketSize the timerBucketSize to set
     */
    public void setTimerBucketSize(int timerBucketSize) {
        this.timerBucketSize = timerBucketSize;
    }

    public void disableMetrics() {
	    enableMetrics = false;
	}

    /**
     * @return the nsEndpoint
     */
    public String getNsEndpoint() {
        return nsEndpoint;
    }

    /**
     * @param nsEndpoint the nsEndpoint to set
     */
    public void setNsEndpoint(String nsEndpoint) {
        this.nsEndpoint = nsEndpoint;
    }

    @Deprecated
    public void setTableInfoCompressed(boolean tableInfoCompressed) {
    }

    public boolean isRemoveDuplicateByTime() {
        return removeDuplicateByTime;
    }

    public void setRemoveDuplicateByTime(boolean removeDuplicateByTime) {
        this.removeDuplicateByTime = removeDuplicateByTime;
    }

    public int getTraverseLimit() {
        return traverseLimit;
    }

    public void setTraverseLimit(int traverseLimit) {
        this.traverseLimit = traverseLimit;
    }

    public int getMaxRetryCnt() {
        return maxRetryCnt;
    }

    public void setMaxRetryCnt(int maxRetryCnt) {
        this.maxRetryCnt = maxRetryCnt;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public int getMaxCntCnnPerHost() {
        return maxCntCnnPerHost;
    }

    public void setMaxCntCnnPerHost(int maxCntCnnPerHost) {
        this.maxCntCnnPerHost = maxCntCnnPerHost;
    }

    public void setZkRootPath(String zkRootPath) {
        this.zkRootPath = zkRootPath;
        this.zkNodeRootPath = zkRootPath + "/nodes";
        this.zkTableRootPath = zkRootPath + "/table/table_data";
        this.zkTableNotifyPath = zkRootPath + "/table/notify";
    }

    public String getZkRootPath() {
        return zkRootPath;
    }

    public String getZkNodeRootPath() {
        return zkNodeRootPath;
    }

    public void setZkNodeRootPath(String zkNodeRootPath) {
        this.zkNodeRootPath = zkNodeRootPath;
    }

    public int getIoThreadNum() {
        return ioThreadNum;
    }

    public void setIoThreadNum(int ioThreadNum) {
        this.ioThreadNum = ioThreadNum;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getZkSesstionTimeout() {
        return zkSesstionTimeout;
    }

    public void setZkSesstionTimeout(int zkSesstionTimeout) {
        this.zkSesstionTimeout = zkSesstionTimeout;
    }

    public void enableMetrics() {
        enableMetrics = true;
	}
	
	public boolean isMetricsEnabled() {
		return enableMetrics;
	}

    public String getZkEndpoints() {
        return zkEndpoints;
    }

    public  void setZkEndpoints(String zkEndpoints) {
        this.zkEndpoints = zkEndpoints;
    }

    public String getZkTableRootPath() {
        return zkTableRootPath;
    }

    public void setZkTableRootPath(String zkTableRootPath) {
        this.zkTableRootPath = zkTableRootPath;
    }

    public String getZkTableNotifyPath() {
        return zkTableNotifyPath;
    }

    public void setZkTableNotifyPath(String zkTableNotifyPath) {
        this.zkTableNotifyPath = zkTableNotifyPath;
    }

    public Map<String, ReadStrategy> getReadStrategies() {
        return readStrategies;
    }

    public void setReadStrategies(Map<String, ReadStrategy> readStrategies) {
        this.readStrategies = readStrategies;
    }
}

package com._4paradigm.rtidb.client.ha;

public class RTIDBClientConfig {

    public enum Mode {
        kSingleNodeMode, kClusterMode
    }
	private boolean enableMetrics = true;
	private String zkEndpoints = "";
	private String zkTableRootPath = "";
	private String zkTableNotifyPath = "";
	private String zkNodeRootPath = "";
	private int zkSesstionTimeout = 10000;
	private int ioThreadNum = 2;
	private int writeTimeout = 10000;
	private int readTimeout = 10000;
	private int maxCntCnnPerHost = 2;
	private Mode mode;
	public void disableMetrics() {
	    enableMetrics = false;
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

    
}

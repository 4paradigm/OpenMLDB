package com._4paradigm.rtidb.client.ha;

public class RTIDBClientConfig {

	private static boolean ENABLE_METRICS = true;
	private static String zkEndpoints = "";
	private static String zkTableRootPath = "";
	private static String zkTableNotifyPath = "";
	private static String zkNodeRootPath = "";
	private static int zkSesstionTimeout = 10000;
	private static int ioThreadNum = 2;
	private static int writeTimeout = 10000;
	private static int readTimeout = 10000;
	private static int maxCntCnnPerHost = 2;
	
	public static void disableMetrics() {
		ENABLE_METRICS = false;
	}
	
	public static int getMaxCntCnnPerHost() {
        return maxCntCnnPerHost;
    }

    public static void setMaxCntCnnPerHost(int maxCntCnnPerHost) {
        RTIDBClientConfig.maxCntCnnPerHost = maxCntCnnPerHost;
    }

    public static String getZkNodeRootPath() {
        return zkNodeRootPath;
    }

    public static void setZkNodeRootPath(String zkNodeRootPath) {
        RTIDBClientConfig.zkNodeRootPath = zkNodeRootPath;
    }

    public static int getIoThreadNum() {
        return ioThreadNum;
    }

    public static void setIoThreadNum(int ioThreadNum) {
        RTIDBClientConfig.ioThreadNum = ioThreadNum;
    }

    public static int getWriteTimeout() {
        return writeTimeout;
    }

    public static void setWriteTimeout(int writeTimeout) {
        RTIDBClientConfig.writeTimeout = writeTimeout;
    }

    public static int getReadTimeout() {
        return readTimeout;
    }

    public static void setReadTimeout(int readTimeout) {
        RTIDBClientConfig.readTimeout = readTimeout;
    }

    public static int getZkSesstionTimeout() {
        return zkSesstionTimeout;
    }

    public static void setZkSesstionTimeout(int zkSesstionTimeout) {
        RTIDBClientConfig.zkSesstionTimeout = zkSesstionTimeout;
    }

    public static void enableMetrics() {
		ENABLE_METRICS = true;
	}
	
	public static boolean isMetricsEnabled() {
		return ENABLE_METRICS;
	}

    public static String getZkEndpoints() {
        return zkEndpoints;
    }

    public static void setZkEndpoints(String zkEndpoints) {
        RTIDBClientConfig.zkEndpoints = zkEndpoints;
    }

    public static String getZkTableRootPath() {
        return zkTableRootPath;
    }

    public static void setZkTableRootPath(String zkTableRootPath) {
        RTIDBClientConfig.zkTableRootPath = zkTableRootPath;
    }

    public static String getZkTableNotifyPath() {
        return zkTableNotifyPath;
    }

    public static void setZkTableNotifyPath(String zkTableNotifyPath) {
        RTIDBClientConfig.zkTableNotifyPath = zkTableNotifyPath;
    }

    
}

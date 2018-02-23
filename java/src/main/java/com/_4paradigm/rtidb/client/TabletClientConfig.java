package com._4paradigm.rtidb.client;

public class TabletClientConfig {

	private static boolean ENABLE_METRICS = true;
	
	
	public static void disableMetrics() {
		ENABLE_METRICS = false;
	}
	
	public static void enableMetrics() {
		ENABLE_METRICS = true;
	}
	
	public static boolean isMetricsEnabled() {
		return ENABLE_METRICS;
	}
}

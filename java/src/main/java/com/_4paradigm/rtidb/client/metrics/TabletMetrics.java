package com._4paradigm.rtidb.client.metrics;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletMetrics {
	private final static Logger logger = LoggerFactory.getLogger(TabletMetrics.class);
	private volatile LinkedList[] latencies = new LinkedList[4];
	private int maxLength = 10240;
	private ScheduledExecutorService sched = Executors.newScheduledThreadPool(1);
	private static TabletMetrics G_METRICS = null;
	public static TabletMetrics getInstance() {
		if (G_METRICS != null) {
			return G_METRICS;
		}
		synchronized (TabletMetrics.class) {
			if (G_METRICS != null) {
				return G_METRICS;
			}
			G_METRICS = new TabletMetrics();
			return G_METRICS;
		}
	}
	private TabletMetrics() {
		LinkedList[] newLatencies = new LinkedList[4];
		for (int i = 0; i < newLatencies.length; i++) {
			newLatencies[i] = new LinkedList<Long>();
		}
		latencies = newLatencies;
		showMetrics();
	}
	
	public synchronized void addPut(Long encode, Long network) {
		addMetrics(0, encode);
		addMetrics(1, network);
	}
	
	private void addMetrics(int index, Long metrics) {
		latencies[index].addLast(metrics);
		if (latencies[index].size() > maxLength) {
			latencies[index].removeFirst();
		}
	}
	
	
	
	public synchronized void addScan(Long decode, Long network) {
		addMetrics(2, decode);
		addMetrics(3, network);
	}
	
	private void showMetrics() {
		try {
			LinkedList[] oldLatencies = latencies;
			LinkedList[] newLatencies = new LinkedList[4];
			for (int i = 0; i < newLatencies.length; i++) {
				newLatencies[i] = new LinkedList<Long>();
			}
			latencies = newLatencies;
			showSingleMetrics("PutEncode", oldLatencies[0]);
			showSingleMetrics("PutRemote", oldLatencies[1]);
			showSingleMetrics("ScanDecode", oldLatencies[2]);
			showSingleMetrics("ScanRemote", oldLatencies[3]);	
		} finally {
			sched.schedule(new Runnable() {
				
				@Override
				public void run() {
					showMetrics();
					
				}
			}, 1, TimeUnit.MINUTES);
		}
		
		
	}
	
	private void showSingleMetrics(String label, LinkedList data) {
		if (data.size() <= 0) {
			return;
		}
		Collections.sort(data);
		int index9999 = Math.min(data.size() - 1, Math.round(data.size() * 0.9999f));
		int index999 = Math.min(data.size() - 1, Math.round(data.size() * 0.999f));
		int index99 = Math.min(data.size() - 1, Math.round(data.size() * 0.99f));
		int index90 = Math.min(data.size() - 1, Math.round(data.size() * 0.90f));
		int index50 = Math.min(data.size() - 1, Math.round(data.size() * 0.5f));
		logger.info("[Metrics] {} ctime {} tp9999 {}us, tp999 {}us, tp99 {}us, tp90 {}us, tp50 {}us", 
				label,System.currentTimeMillis(), (Long)data.get(index9999) /1000l, (Long) data.get(index999)/1000l,
				(Long)data.get(index99) / 1000l, (Long)data.get(index90)/1000l, (Long)data.get(index50)/1000l);
	}
	
	public void close() {
		sched.shutdownNow();
	}
}

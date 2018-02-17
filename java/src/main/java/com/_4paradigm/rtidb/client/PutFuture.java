package com._4paradigm.rtidb.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.PutResponse;

public class PutFuture implements Future<Boolean>{

	private Future<Tablet.PutResponse> f;
	private Long startTime = -1l;
	public PutFuture(Future<Tablet.PutResponse> f) {
		this.f = f;
	}
	
	public PutFuture(Future<Tablet.PutResponse> f, Long startTime) {
		this.f = f;
		this.startTime = startTime;
	}
	
	public static PutFuture wrapper(Future<Tablet.PutResponse> f) {
		return new PutFuture(f);
	}
	
	public static PutFuture wrapper(Future<Tablet.PutResponse> f, Long startTime) {
		return new PutFuture(f, startTime);
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return f.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return f.isCancelled();
	}

	@Override
	public boolean isDone() {
		return f.isDone();
	}

	@Override
	public Boolean get() throws InterruptedException, ExecutionException {
		PutResponse response = f.get();
		
		if (startTime > 0) {
			Long network = System.nanoTime() - startTime;
			if(RTIDBClientConfig.isMetricsEnabled()) {
				TabletMetrics.getInstance().addPut(-1l, network);
			}
		}
		if (response != null && response.getCode() == 0) {
			return true;
		}
		return false;
	}

	@Override
	public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		PutResponse response = f.get(timeout, unit);
		if (response != null && response.getCode() == 0) {
			return true;
		}
		return false;
	}

}

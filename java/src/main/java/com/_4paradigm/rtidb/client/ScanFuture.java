package com._4paradigm.rtidb.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.schema.Table;

import rtidb.api.Tablet;
import rtidb.api.Tablet.ScanResponse;

public class ScanFuture implements Future<KvIterator>{

	private Future<Tablet.ScanResponse> f;
	private Table t;
	private Long startTime = -1l;
	public ScanFuture(Future<Tablet.ScanResponse> f, Table t) {
		this.f = f;
		this.t = t;
	}
	
	public ScanFuture(Future<Tablet.ScanResponse> f, Table t, Long startTime) {
		this.f = f;
		this.t = t;
		this.startTime = startTime;
	}
	
	public static ScanFuture wrappe(Future<Tablet.ScanResponse> f, Table t) {
		return new ScanFuture(f, t);
	}
	
	public static ScanFuture wrappe(Future<Tablet.ScanResponse> f, Table t, Long startTime) {
		return new ScanFuture(f, t, startTime);
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
	public KvIterator get() throws InterruptedException, ExecutionException {
		ScanResponse response = f.get();
		Long network = -1l;
		if (startTime > 0) {
			network = System.nanoTime() - startTime;
		}
		if (response != null ) {
			if (response.getCode() == 0) {
				if (t != null) {
					return new KvIterator(response.getPairs(), t.getSchema(), network);
				}
				return new KvIterator(response.getPairs(), network);
			}
			return null;
		}
		throw new ExecutionException("Internal server error", null);
	}

	@Override
	public KvIterator get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		ScanResponse response = f.get(timeout, unit);
		Long network = -1l;
		if (startTime > 0) {
			network = System.nanoTime() - startTime;
		}
		if (response != null && response.getCode() == 0) {
			KvIterator it = null;
			if (t != null) {
				it = new KvIterator(response.getPairs(), t.getSchema(), network);
			}
			it = new KvIterator(response.getPairs(), network);
			it.setCount(response.getCount());
			return it;
		}
		throw new ExecutionException("Internal server error", null);
	}

}

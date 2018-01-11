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
	public ScanFuture(Future<Tablet.ScanResponse> f, Table t) {
		this.f = f;
		this.t = t;
	}
	
	public static ScanFuture wrappe(Future<Tablet.ScanResponse> f, Table t) {
		return new ScanFuture(f, t);
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
		if (response != null ) {
			if (response.getCode() == 0) {
				if (t != null) {
					return new KvIterator(response.getPairs(), t.getSchema());
				}
				return new KvIterator(response.getPairs());
			}
			return null;
		}
		throw new ExecutionException("Internal server error", null);
	}

	@Override
	public KvIterator get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		ScanResponse response = f.get(timeout, unit);
		if (response != null && response.getCode() == 0) {
			return new KvIterator(response.getPairs());
		}
		throw new ExecutionException("Internal server error", null);
	}

}

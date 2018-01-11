package com._4paradigm.rtidb.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;

import rtidb.api.Tablet;
import rtidb.api.Tablet.GetResponse;

public class GetFuture implements Future<ByteString>{
	private Future<Tablet.GetResponse> f;

	public static GetFuture wrappe(Future<Tablet.GetResponse> f) {
		return new GetFuture(f);
	}
	
	public GetFuture(Future<Tablet.GetResponse> f) {
		this.f = f;
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
	public ByteString get() throws InterruptedException, ExecutionException {
		GetResponse response = f.get();
		if (response != null && response.getCode() == 0) {
			return response.getValue();
		}
		return null;
	}

	@Override
	public ByteString get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		GetResponse response = f.get(timeout, unit);
		if (response != null && response.getCode() == 0) {
			return response.getValue();
		}
		return null;
	}

}

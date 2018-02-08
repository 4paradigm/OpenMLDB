package com._4paradigm.rtidb.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.Table;
import com.google.protobuf.ByteString;

import rtidb.api.Tablet;
import rtidb.api.Tablet.GetResponse;

public class GetFuture implements Future<ByteString>{
	private Future<Tablet.GetResponse> f;
	private Table t;
	private long startTime;
	public static GetFuture wrappe(Future<Tablet.GetResponse> f, long startTime) {
		return new GetFuture(f, startTime);
	}
	
	public static GetFuture wrappe(Future<Tablet.GetResponse> f, Table t, long startTime) {
		return new GetFuture(f, t, startTime);
	}
	
	public GetFuture(Future<Tablet.GetResponse> f, long startTime) {
		this.f = f;
		this.startTime = startTime;
	}
	
	public GetFuture(Future<Tablet.GetResponse> f, Table t, long startTime) {
		this.f = f;
		this.t = t;
		this.startTime = startTime;
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

	public Object[] getRow(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TabletException, TimeoutException {
		if (t == null || t.getSchema().isEmpty()) {
			throw new TabletException("no schema for table " + t);
		}
		ByteString raw = get(timeout, unit);
		long network = System.nanoTime() - startTime;
		long decode = System.nanoTime();
		Object[] row = RowCodec.decode(raw.asReadOnlyByteBuffer(), t.getSchema());
		if (TabletClientConfig.isMetricsEnabled()) {
		    decode = System.nanoTime() - decode;
		    TabletMetrics.getInstance().addGet(decode, network);
		}
		return row;
	}
	
	public Object[] getRow() throws InterruptedException, ExecutionException, TabletException{
	    if (t == null || t.getSchema().isEmpty()) {
            throw new TabletException("no schema for table " + t);
        }
        ByteString raw = get();
        long network = System.nanoTime() - startTime;
        long decode = System.nanoTime();
        Object[] row = RowCodec.decode(raw.asReadOnlyByteBuffer(), t.getSchema());
        if (TabletClientConfig.isMetricsEnabled()) {
            decode = System.nanoTime() - decode;
            TabletMetrics.getInstance().addGet(decode, network);
        }
        return row;
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

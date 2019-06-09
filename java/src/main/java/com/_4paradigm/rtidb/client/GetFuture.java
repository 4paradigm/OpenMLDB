package com._4paradigm.rtidb.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.GetResponse;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

public class GetFuture implements Future<ByteString>{
	private Future<Tablet.GetResponse> f;
	private TableHandler t;
	private long startTime = 0;
	private RTIDBClientConfig config = null;
	
	public static GetFuture wrappe(Future<Tablet.GetResponse> f, long startTime, RTIDBClientConfig config) {
        return new GetFuture(f, startTime, config);
    }
    
    public static GetFuture wrappe(Future<Tablet.GetResponse> f, TableHandler t, long startTime, RTIDBClientConfig config) {
        return new GetFuture(f, t, startTime, config);
    }
	
	public GetFuture(Future<Tablet.GetResponse> f, TableHandler t, long startTime, RTIDBClientConfig config) {
		this.f = f;
		this.t = t;
		this.startTime = startTime;
		this.config = config;
	}
	
	public GetFuture(Future<Tablet.GetResponse> f,  long startTime, RTIDBClientConfig config) {
        this.f = f;
        this.startTime = startTime;
        this.config = config;
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
		if (raw == null) {
			throw new TabletException("get failed");
		}
		Object[] row = new Object[t.getSchema().size()];
		decode(raw, row, 0, row.length);
		return row;
	}
	
	public Object[] getRow() throws InterruptedException, ExecutionException, TabletException{
	    if (t == null || t.getSchema().isEmpty()) {
            throw new TabletException("no schema for table " + t);
        }
	    Object[] row = new Object[t.getSchema().size()];
	    getRow(row, 0, row.length);
	    return row;
	}
	
	public void getRow(Object[] row, int start, int length) throws TabletException, InterruptedException, ExecutionException {
	    if (t == null || t.getSchema().isEmpty()) {
            throw new TabletException("no schema for table " + t);
        }
        ByteString raw = get();
        if (raw == null) {
            return ;
        }
        decode(raw, row, start, length);
	}
	
	private void decode(ByteString raw, Object[] row, int start, int length) throws TabletException {
	    RowCodec.decode(raw.asReadOnlyByteBuffer(), t.getSchema(), row, start, length);
	}
	
	@Override
	public ByteString get() throws InterruptedException, ExecutionException {
		GetResponse response = f.get();
		if (response != null && response.getCode() == 0) {
			if (t.getTableInfo().hasCompressType() && t.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
				byte[] uncompressed = Compress.snappyUnCompress(response.getValue().toByteArray());
				return ByteString.copyFrom(uncompressed);
			} else {
				return response.getValue();
			}
		}
		if (response != null) {
			String msg = String.format("Bad request with error %s code %d", response.getMsg(), response.getCode());
			throw new ExecutionException(msg, null);
		} else {
			throw new ExecutionException("response is null", null);
		}
	}

	@Override
	public ByteString get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		GetResponse response = f.get(timeout, unit);
		if (response != null && response.getCode() == 0) {
			if (t.getTableInfo().hasCompressType() && t.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
				byte[] uncompressed = Compress.snappyUnCompress(response.getValue().toByteArray());
				return ByteString.copyFrom(uncompressed);
			} else {
				return response.getValue();
			}
		}
		return null;
	}

}

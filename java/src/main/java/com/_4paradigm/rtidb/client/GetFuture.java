package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.*;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.GetResponse;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GetFuture implements Future<ByteString>{
	private Future<Tablet.GetResponse> f;
	private TableHandler th;
	private RowView rv;
	private List<ColumnDesc> projection;
	private ProjectionInfo projectionInfo = null;

	private int rowLength;
	public static GetFuture wrappe(Future<Tablet.GetResponse> f, RTIDBClientConfig config) {
		return new GetFuture(f, config);
	}

	public static GetFuture wrappe(Future<Tablet.GetResponse> f, TableHandler t, RTIDBClientConfig config) {
		return new GetFuture(f, t);
	}
	// for legacy format
	public GetFuture(Future<Tablet.GetResponse> f, TableHandler t, ProjectionInfo projectionInfo) {
		this.f = f;
		this.th = t;
		this.projectionInfo = projectionInfo;
		this.rowLength = projectionInfo.getProjectionCol().size();
	}

	public GetFuture(Future<Tablet.GetResponse> f, TableHandler t) {
		this.f = f;
		this.th = t;
		if (t != null && t.getTableInfo().getFormatVersion() == 1) {
			rv = new RowView(t.getSchema());
		}
		rowLength = t.getSchema().size();
		if (th.getSchemaMap().size() > 0) {
			rowLength += th.getSchemaMap().size();
		}
	}

	public GetFuture(Future<Tablet.GetResponse> f, TableHandler t, RTIDBClientConfig config, List<ColumnDesc> projection) {
		this.f = f;
		this.th = t;
		rowLength = t.getSchema().size();
		if (t != null && t.getTableInfo().getFormatVersion() == 1) {
			if (projection != null) {
				rv = new RowView(projection);
				rowLength = projection.size();
			}else {
				rv = new RowView(t.getSchema());
			}
			this.projection = projection;
		}

	}

	public GetFuture(Future<Tablet.GetResponse> f,  RTIDBClientConfig config) {
		this.f = f;
	}


	public static GetFuture wrappe(Future<Tablet.GetResponse> f, long _, RTIDBClientConfig config) {
		return new GetFuture(f, config);
	}

	public GetFuture(Future<Tablet.GetResponse> f, TableHandler t, long _, RTIDBClientConfig config) {
		this.f = f;
		this.th = t;
		if (t.getTableInfo().getFormatVersion() == 1) {
			rv = new RowView(t.getSchema());
		}
	}
	public GetFuture() {}

	public GetFuture(Future<Tablet.GetResponse> f, long _, RTIDBClientConfig config) {
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

	public Object[] getRow(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TabletException, TimeoutException {
		if (th.getSchema() == null || th.getSchema().isEmpty()) {
			throw new TabletException("no schema for table " + th.getTableInfo().getName());
		}
		ByteString raw = get(timeout, unit);
		if (raw == null || raw.isEmpty()) {
			return null;
		}
		Object[] row = new Object[rowLength];
		decode(raw, row, 0, row.length);
		return row;
	}

	public Object[] getRow() throws InterruptedException, ExecutionException, TabletException{
		if (th.getSchema() == null || th.getSchema().isEmpty()) {
			throw new TabletException("no schema for table " + th.getTableInfo().getName());
		}
		ByteString raw = get();
		if (raw == null || raw.isEmpty()) {
			return null;
		}
		Object[] row = new Object[rowLength];
		decode(raw, row, 0, row.length);
		return row;
	}

	@Deprecated
	public void getRow(Object[] row, int start, int length) throws TabletException, InterruptedException, ExecutionException {
		if (th.getSchema() == null || th.getSchema().isEmpty()) {
			throw new TabletException("no schema for table " + th.getTableInfo().getName());
		}
		ByteString raw = get();
		if (raw == null || raw.isEmpty()) {
		    return;
		}
		decode(raw, row, start, length);
	}

	private void decode(ByteString raw, Object[] row, int start, int length) throws TabletException {
	    switch (th.getTableInfo().getFormatVersion()) {
			case 1:
				rv.read(raw.asReadOnlyByteBuffer().order(ByteOrder.LITTLE_ENDIAN), row, start, length);
				break;
			default:
			    if (projectionInfo != null && projectionInfo.getProjectionCol() != null &&
						!projectionInfo.getProjectionCol().isEmpty()) {
					RowCodec.decode(raw.asReadOnlyByteBuffer(), th.getSchema(), projectionInfo, row, start, length);
				}else {
					RowCodec.decode(raw.asReadOnlyByteBuffer(), th.getSchema(), row, start, length);
				}
		}
	}

	@Override
	public ByteString get() throws InterruptedException, ExecutionException {
		GetResponse response = f.get();
		if (response != null && response.getCode() == 0) {
			if (th.getTableInfo().hasCompressType()  && th.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
				byte[] uncompressed = Compress.snappyUnCompress(response.getValue().toByteArray());
				return ByteString.copyFrom(uncompressed);
			} else {
				return response.getValue();
			}
		}
		if (response != null) {
			if (response.getCode() == 109) {
				return null;
			}
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
			if (th.getTableInfo().hasCompressType()  && th.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
				byte[] uncompressed = Compress.snappyUnCompress(response.getValue().toByteArray());
				return ByteString.copyFrom(uncompressed);
			} else {
				return response.getValue();
			}
		}
		if (response.getCode() == 109) {
			return null;
		}
		if (response != null) {
			String msg = String.format("Bad request with error %s code %d", response.getMsg(), response.getCode());
			throw new ExecutionException(msg, null);
		} else {
			throw new ExecutionException("response is null", null);
		}
	}

}

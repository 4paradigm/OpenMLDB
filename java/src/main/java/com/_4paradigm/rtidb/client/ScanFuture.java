package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.impl.DefaultKvIterator;
import com._4paradigm.rtidb.client.impl.RowKvIterator;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ProjectionInfo;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.ScanResponse;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ScanFuture implements Future<KvIterator> {

    private Future<Tablet.ScanResponse> f;
    private TableHandler t;
    private ProjectionInfo projectionInfo = null;

    public ScanFuture(Future<Tablet.ScanResponse> f, TableHandler t) {
        this.f = f;
        this.t = t;
    }

    public ScanFuture(Future<Tablet.ScanResponse> f, TableHandler t, ProjectionInfo projectionInfo) {
        this.f = f;
        this.t = t;
        this.projectionInfo = projectionInfo;
    }

    public ScanFuture(Future<Tablet.ScanResponse> f, TableHandler t, Long startTime) {
        this.f = f;
        this.t = t;
    }

    public ScanFuture() {}

    public static ScanFuture wrappe(Future<Tablet.ScanResponse> f, TableHandler t) {
        return new ScanFuture(f, t);
    }

    public static ScanFuture wrappe(Future<Tablet.ScanResponse> f, TableHandler t, Long startTime) {
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
        if (response == null) {
            throw new ExecutionException("Connection error", null);
        }
        if (response.getCode() == 0) {
            switch (t.getTableInfo().getFormatVersion()) {
                case 1:
                    return getNewKvIterator(response);
                default:
                    return getLegacyKvIterator(response);
            }
        }
        String msg = String.format("Bad request with error %s code %d", response.getMsg(), response.getCode());
        throw new ExecutionException(msg, null);
    }

    public ProjectionInfo getProjectionInfo() {
        return projectionInfo;
    }

    private KvIterator getLegacyKvIterator(ScanResponse response) {
        DefaultKvIterator kit = null;
        if (t != null) {
            if (projectionInfo != null && projectionInfo.getProjectionCol() != null &&
                !projectionInfo.getProjectionCol().isEmpty()) {
                kit = new DefaultKvIterator(response.getPairs(), t.getSchema(), projectionInfo);
            }else if (t.getSchemaMap().size() > 0) {
                kit = new DefaultKvIterator(response.getPairs(), t);
            } else {
                kit = new DefaultKvIterator(response.getPairs(), t.getSchema());
            }
            if (t.getTableInfo().hasCompressType()) {
                kit.setCompressType(t.getTableInfo().getCompressType());
            }
        }else {
            kit = new DefaultKvIterator(response.getPairs());
        }
        kit.setCount(response.getCount());
        return kit;
    }

    private KvIterator getNewKvIterator(ScanResponse response) {
        RowKvIterator kit = null;
        if (projectionInfo != null && projectionInfo.getProjectionSchema() != null) {
            kit = new RowKvIterator(response.getPairs(), projectionInfo.getProjectionSchema(), response.getCount(), true);
        }else {
            kit = new RowKvIterator(response.getPairs(), t.getSchema(), response.getCount());
            if (t.getSchemaMap().size() > 0) {
                kit.setVerMap(t.getVersions());
                kit.setSchemaMap(t.getSchemaMap());
            }
        }
        kit.setCount(response.getCount());
        kit.setCompressType(t.getTableInfo().getCompressType());
        return kit;

    }

    public ScanResponse getResponse() throws InterruptedException, ExecutionException {
        return f.get();
    }

    @Override
    public KvIterator get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        ScanResponse response = f.get(timeout, unit);
        if (response == null) {
            throw new ExecutionException("Connection error", null);
        }
        if (response.getCode() == 0) {
            switch (t.getTableInfo().getFormatVersion()) {
                case 1:
                    return getNewKvIterator(response);
                default:
                    return getLegacyKvIterator(response);
            }
        }
        String msg = String.format("Bad request with error %s code %d", response.getMsg(), response.getCode());
        throw new ExecutionException(msg, null);
    }

}

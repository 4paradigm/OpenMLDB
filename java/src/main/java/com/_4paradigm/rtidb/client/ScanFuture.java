package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.impl.DefaultKvIterator;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.ScanResponse;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ScanFuture implements Future<KvIterator> {

    private Future<Tablet.ScanResponse> f;
    private TableHandler t;

    public ScanFuture(Future<Tablet.ScanResponse> f, TableHandler t) {
        this.f = f;
        this.t = t;
    }

    public ScanFuture(Future<Tablet.ScanResponse> f, TableHandler t, Long startTime) {
        this.f = f;
        this.t = t;
    }

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
        Long network = -1l;
        if (response == null) {
            throw new ExecutionException("Connection error", null);
        }
        if (response.getCode() == 0) {
            DefaultKvIterator kit = null;
            if (t != null) {
                if (t.getSchemaMap().size() > 0) {
                    kit = new DefaultKvIterator(response.getPairs(), t);
                } else {
                    kit = new DefaultKvIterator(response.getPairs(), t.getSchema(), network);
                }
                if (t.getTableInfo().hasCompressType()) {
                    kit.setCompressType(t.getTableInfo().getCompressType());
                }
            }else {
                kit = new DefaultKvIterator(response.getPairs(), network);
            }
            kit.setCount(response.getCount());
            return kit;
        }
        String msg = String.format("Bad request with error %s code %d", response.getMsg(), response.getCode());
        throw new ExecutionException(msg, null);
    }

    @Override
    public KvIterator get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        ScanResponse response = f.get(timeout, unit);
        Long network = -1l;
        if (response == null) {
            throw new ExecutionException("Connection error", null);
        }
        if (response.getCode() == 0) {
            DefaultKvIterator kit = null;
            if (t != null) {
                if (t.getSchemaMap().size() > 0) {
                    kit = new DefaultKvIterator(response.getPairs(), t);
                } else {
                    kit = new DefaultKvIterator(response.getPairs(), t.getSchema(), network);
                }
                if (t.getTableInfo().hasCompressType()) {
                    kit.setCompressType(t.getTableInfo().getCompressType());
                }
            }else {
                kit = new DefaultKvIterator(response.getPairs(), network);
            }
            kit.setCount(response.getCount());
            return kit;
        }
        String msg = String.format("Bad request with error %s code %d", response.getMsg(), response.getCode());
        throw new ExecutionException(msg, null);

    }

}

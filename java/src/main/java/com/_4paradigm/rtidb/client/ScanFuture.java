package com._4paradigm.rtidb.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com._4paradigm.rtidb.client.schema.Table;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.ScanResponse;

public class ScanFuture implements Future<KvIterator> {

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
        if (response == null) {
            throw new ExecutionException("Connection error", null);
        }

        if (response.getCode() == 0) {
            KvIterator kit = null;
            if (t != null) {
                kit = new KvIterator(response.getPairs(), t.getSchema(), network);
            }else {
                kit = new KvIterator(response.getPairs(), network);
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
        if (startTime > 0) {
            network = System.nanoTime() - startTime;
        }
        if (response == null) {
            throw new ExecutionException("Connection error", null);
        }
        if (response.getCode() == 0) {
            KvIterator kit = null;
            if (t != null) {
                kit = new KvIterator(response.getPairs(), t.getSchema(), network);
            }else {
                kit = new KvIterator(response.getPairs(), network);
            }
            kit.setCount(response.getCount());
            return kit;
        }
        String msg = String.format("Bad request with error %s code %d", response.getMsg(), response.getCode());
        throw new ExecutionException(msg, null);

    }

}
